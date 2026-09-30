from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_month_csat_by_csat = Window.partitionBy(F.month("closed_at"), "csat").orderBy("csat", "id").rowsBetween(-3, Window.currentRow)
w_csat_closed_at_date_year_by = Window.partitionBy("csat", F.year("closed_at_date")).orderBy(F.col("closed_at_date").desc())
w_agent_id_closed_at_month_by = Window.partitionBy("agent_id", F.date_trunc("month", "closed_at")).orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_requester_id_tier = Window.partitionBy("agent_id", "requester_id_tier")
w_closed_at_year_requester_id = Window.partitionBy(F.year("closed_at"), "requester_id_tier")
w_channel_by_closed_at_date_ru = Window.partitionBy("channel").orderBy(F.col("closed_at_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_closed_at_month_by_2 = Window.partitionBy("agent_id", F.date_trunc("month", "closed_at")).orderBy(F.col("closed_at").desc(), "id").rowsBetween(-2, -1)
w_agent_id_closed_at_month_by_3 = Window.partitionBy("agent_id", F.date_trunc("month", "closed_at")).orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.when(F.col("requester_id") >= 18, "HIGH").when(F.col("requester_id") >= 7, "MEDIUM").otherwise("LOW").alias("requester_id_tier"),
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
)

windowed = (
    valid_tickets
    .select(
        "id",
        "requester_id",
        "closed_at",
        "channel",
        "csat",
        "requester_id_tier",
        "closed_at_date",
        F.sum("csat").over(w_closed_at_month_csat_by_csat).alias("csat_sum_4"),
        F.round(F.percent_rank().over(w_csat_closed_at_date_year_by), 3).alias("closed_at_date_percent_rank"),
        F.nth_value("requester_id", 2).over(w_agent_id_closed_at_month_by).alias("second_requester_id"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id_requester_id_tier), 2).alias("requester_id_vs_agent_id_requester_id_tier_avg"),
        F.sum("csat").over(w_agent_id_requester_id_tier).alias("agent_id_requester_id_tier_sum_csat"),
        F.max("requester_id").over(w_closed_at_year_requester_id).alias("closed_at_year_requester_id_tier_max_requester_i"),
        F.count(F.lit(1)).over(w_channel_by_closed_at_date_ru).alias("running_count"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_closed_at_year_requester_id), F.lit(0)), 4).alias("requester_id_share_of_closed_at_year_requester_i"),
        F.round(F.avg("csat").over(w_closed_at_year_requester_id), 2).alias("closed_at_year_requester_id_tier_avg_csat"),
        F.sum("requester_id").over(w_agent_id_closed_at_month_by_2).alias("requester_id_sum_2_prior"),
        F.count(F.lit(1)).over(w_agent_id_requester_id_tier).alias("rows_in_agent_id_requester_id_tier"),
        F.last("closed_at_date", ignorenulls=True).over(w_agent_id_closed_at_month_by_3).alias("last_known_closed_at_date"),
        F.sum("requester_id").over(w_agent_id_requester_id_tier).alias("agent_id_requester_id_tier_sum_requester_id"),
    )
)

result = (
    windowed
    .select(
        "id",
        "requester_id",
        "closed_at",
        "channel",
        "csat",
        "csat_sum_4",
        "agent_id_requester_id_tier_sum_csat",
        "requester_id_share_of_closed_at_year_requester_i",
        "closed_at_year_requester_id_tier_avg_csat",
        "requester_id_sum_2_prior",
        "rows_in_agent_id_requester_id_tier",
        "last_known_closed_at_date",
        (F.col("second_requester_id") - F.col("requester_id_sum_2_prior")).alias("second_requester_id_minus_requester_id_sum_2_pri"),
    )
    .orderBy("requester_id", "id")
    .limit(20)
)
