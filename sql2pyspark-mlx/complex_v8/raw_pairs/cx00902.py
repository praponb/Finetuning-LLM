from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_all_by_requester_id_full = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_requester_id_ties = Window.orderBy(F.col("requester_id").desc())

prepared_tickets = (
    df_tickets
    .filter((F.col("requester_id").between(5, 23)) & (F.size(F.col("tags")) >= 1))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

windowed = (
    prepared_tickets
    .select(
        "*",
        F.size(F.collect_set("channel").over(w_priority)).alias("distinct_channel_in_priority"),
        F.nth_value("opened_at", 2).over(w_all_by_requester_id_full).alias("second_opened_at"),
        F.size(F.collect_set("agent_id").over(w_priority)).alias("distinct_agent_id_in_priority"),
        F.rank().over(w_all_by_requester_id_ties).alias("requester_id_rank"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        (F.floor(F.col("requester_id_rank") / 2) * 2).alias("requester_id_rank_bucket"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id_rank"), F.lit(0)), 3).alias("csat_per_requester_id_rank"),
        F.when(F.col("requester_id") >= 19, "A").when(F.col("requester_id") >= 8, "B").otherwise("C").alias("requester_id_tier"),
    )
)

result = (
    annotated
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "tags",
        "second_opened_at",
        "requester_id_rank",
        "csat_per_requester_id_rank",
        "requester_id_tier",
        F.when(F.col("csat") >= 4, "A").when(F.col("csat") >= 3, "B").otherwise("C").alias("csat_tier"),
        F.round(F.col("requester_id_rank_bucket") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("requester_id_rank_bucket_per_requester_id"),
    )
    .orderBy(
        F.col("requester_id_rank").desc(),
        F.col("csat_per_requester_id_rank").asc(),
    )
)
