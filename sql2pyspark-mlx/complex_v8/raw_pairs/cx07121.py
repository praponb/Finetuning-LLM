from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_csat_by_csat_full = Window.partitionBy("csat").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_requester_id_full = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat_prior_2 = Window.orderBy("csat", "id").rowsBetween(-2, -1)
w_closed_at_month_agent_id = Window.partitionBy(F.date_trunc("month", "closed_at"), "agent_id")
w_all_by_csat = Window.orderBy("csat", "id")
w_closed_at_month_agent_id_by = Window.partitionBy(F.date_trunc("month", "closed_at"), "agent_id").orderBy(F.col("closed_at").asc_nulls_last(), "id")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_closed_at_to_date = Window.partitionBy(F.to_date("closed_at"))

scoped = (
    df_tickets
    .filter((F.col("channel") == "email") & (F.col("csat") > 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        F.concat(F.col("channel"), F.lit("#"), F.col("csat").cast("string")).alias("channel_ref"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        (F.col("csat") - F.lag("csat").over(w_all_by_requester_id)).alias("csat_change"),
        F.nth_value("requester_id", 2).over(w_csat_by_csat_full).alias("second_requester_id"),
        F.nth_value("csat", 2).over(w_all_by_requester_id_full).alias("second_csat"),
        F.row_number().over(w_all_by_requester_id).alias("seq_no"),
        F.round(F.avg("requester_id").over(w_all_by_csat_prior_2), 2).alias("requester_id_avg_2_prior"),
        F.count(F.lit(1)).over(w_closed_at_month_agent_id).alias("rows_in_closed_at_month_agent_id"),
        F.lag("csat").over(w_all_by_csat).alias("prev_csat"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_closed_at_month_agent_id_by))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id_csat), F.lit(0)), 4).alias("csat_share_of_agent_id_csat"),
        F.ntile(4).over(w_all_by_csat).alias("csat_quartile"),
        F.lead("closed_at").over(w_all_by_csat).alias("next_closed_at"),
        F.round(F.col("csat") - F.avg("csat").over(w_closed_at_month_agent_id), 2).alias("csat_vs_closed_at_month_agent_id_avg"),
        F.round(F.col("csat") - F.avg("csat").over(w_closed_at_to_date), 2).alias("csat_vs_closed_at_to_date_avg"),
        F.rank().over(w_closed_at_month_agent_id_by).alias("closed_at_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "requester_id",
        "closed_at",
        "csat",
        "csat_change",
        "second_requester_id",
        "seq_no",
        "requester_id_avg_2_prior",
        "rows_in_closed_at_month_agent_id",
        "minutes_since_prev_closed_at",
        "csat_share_of_agent_id_csat",
        "csat_vs_closed_at_month_agent_id_avg",
        "csat_vs_closed_at_to_date_avg",
        "closed_at_rank",
        F.when(F.col("prev_csat") >= 4, "HIGH").when(F.col("prev_csat") >= 3, "MEDIUM").otherwise("LOW").alias("prev_csat_tier"),
        (F.col("requester_id_avg_2_prior") + F.col("csat_quartile")).alias("requester_id_avg_2_prior_plus_csat_quartile"),
    )
    .orderBy(F.col("second_requester_id").asc(), F.col("seq_no").desc())
)
