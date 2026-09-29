from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat = Window.partitionBy("csat")
w_csat_by_requester_id_full = Window.partitionBy("csat").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_requester_id_ties = Window.partitionBy("csat").orderBy(F.col("requester_id").desc())
w_csat_by_requester_id = Window.partitionBy("csat").orderBy(F.col("requester_id").desc(), "id")
w_agent_id_by_requester_id_pri = Window.partitionBy("agent_id").orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_csat_by_requester_id_2 = Window.partitionBy("csat").orderBy("requester_id", "id")
w_agent_id_by_requester_id = Window.partitionBy("agent_id").orderBy(F.col("requester_id").asc_nulls_last(), "id")
w_closed_at_month_next3_closed = Window.partitionBy(F.month("closed_at"), F.year("next3_closed_at")).orderBy(F.col("prev3_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_month_next3_closed_2 = Window.partitionBy(F.month("closed_at"), F.year("next3_closed_at")).orderBy(F.col("prev3_requester_id").desc(), "id")
w_next3_closed_at_month_agent = Window.partitionBy(F.date_trunc("month", "next3_closed_at"), "agent_id").orderBy(F.col("prev2_csat").desc())
w_distinct_agent_id_in_csat_by = Window.partitionBy("distinct_agent_id_in_csat").orderBy("requester_id", "id").rowsBetween(-6, -1)

valid_tickets = (
    df_tickets
    .filter(F.col("csat") >= 3)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

ranked = (
    valid_tickets
    .select(
        "*",
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_csat), 2).alias("requester_id_vs_csat_avg"),
        F.nth_value("requester_id", 2).over(w_csat_by_requester_id_full).alias("second_requester_id"),
        F.size(F.collect_set("agent_id").over(w_csat)).alias("distinct_agent_id_in_csat"),
        F.dense_rank().over(w_csat_by_requester_id_ties).alias("requester_id_dense_rank"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_csat), F.lit(0)), 4).alias("requester_id_share_of_csat"),
        F.ntile(10).over(w_csat_by_requester_id).alias("requester_id_decile"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.max("requester_id").over(w_agent_id_by_requester_id_pri).alias("requester_id_max_6_prior"),
        F.lag("requester_id", 3, 0).over(w_csat_by_requester_id).alias("prev3_requester_id"),
        F.lead("closed_at", 3).over(w_csat_by_requester_id_2).alias("next3_closed_at"),
        F.lag("csat", 2).over(w_agent_id_by_requester_id).alias("prev2_csat"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.last("requester_id_share_of_csat").over(w_closed_at_month_next3_closed).alias("last_requester_id_share_of_csat"),
        F.lag("prev3_requester_id").over(w_closed_at_month_next3_closed_2).alias("prev_prev3_requester_id"),
        F.dense_rank().over(w_next3_closed_at_month_agent).alias("prev2_csat_dense_rank"),
        F.max("requester_id_max_6_prior").over(w_distinct_agent_id_in_csat_by).alias("requester_id_max_6_prior_max_6_prior"),
    )
)

result = (
    ranked_2
    .filter(F.col("channel_slug").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        "channel_slug",
        "second_requester_id",
        "distinct_agent_id_in_csat",
        "requester_id_dense_rank",
        "requester_id_share_of_csat",
        "requester_id_decile",
        "requester_id_max_6_prior",
        "prev3_requester_id",
        "next3_closed_at",
        "prev2_csat",
        "last_requester_id_share_of_csat",
        "prev_prev3_requester_id",
        "prev2_csat_dense_rank",
        "requester_id_max_6_prior_max_6_prior",
    )
    .orderBy(F.col("requester_id_decile").desc(), "requester_id_max_6_prior")
)
