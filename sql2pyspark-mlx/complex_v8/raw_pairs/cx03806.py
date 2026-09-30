from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id", "channel").orderBy(F.col("csat").desc(), F.col("id").desc())
w_agent_id = Window.partitionBy("agent_id")
w_all_by_closed_at_full = Window.orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.date_trunc("hour", F.col("opened_at")).alias("opened_at_hour"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    deduped
    .select(
        "*",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id), F.lit(0)), 4).alias("csat_share_of_agent_id"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.min("requester_id_bucket").over(w_agent_id).alias("agent_id_min_requester_id_bucket"),
        F.nth_value("requester_id_bucket", 2).over(w_all_by_closed_at_full).alias("second_requester_id_bucket"),
        F.sum("requester_id_bucket").over(w_agent_id).alias("agent_id_sum_requester_id_bucket"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id), 2).alias("csat_vs_agent_id_avg"),
    )
)

result = (
    ranked
    .cube("agent_id", "csat", "priority")
    .agg(
        F.round(F.sum("second_requester_id_bucket") / F.nullif(F.sum("agent_id_min_requester_id_bucket"), F.lit(0)), 3).alias("second_requester_id_bucket_to_agent_id_min_reque"),
        F.max("agent_id_sum_requester_id_bucket").alias("max_agent_id_sum_requester_id_bucket"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("csat").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
    )
)
