from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_all_by_total_requester_id = Window.orderBy(F.col("total_requester_id").desc(), "channel")

base = (
    df_tickets
    .filter(F.col("csat") < 4)
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

stats = (
    base
    .groupBy("channel")
    .agg(
        F.sum("csat").alias("total_csat"),
        F.round(F.sum("requester_id") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("requester_id_to_csat_ratio"),
        F.round(F.avg("csat"), 2).alias("avg_csat"),
        F.sum("requester_id").alias("total_requester_id"),
    )
)

windowed = (
    stats
    .select(
        "channel",
        "total_csat",
        "requester_id_to_csat_ratio",
        "total_requester_id",
        F.round(F.col("total_csat") - F.avg("total_csat").over(w_channel), 2).alias("total_csat_vs_channel_avg"),
        F.rank().over(w_all_by_total_requester_id).alias("total_requester_id_rank"),
    )
)

top_per_group = (
    windowed
    .filter(F.col("total_requester_id_rank") <= 5)
    .drop("total_requester_id_rank")
)

result = (
    top_per_group
    .filter(F.col("channel") == "phone")
    .select(
        "channel",
        "requester_id_to_csat_ratio",
        "total_csat_vs_channel_avg",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
    .orderBy(F.col("channel").desc(), "requester_id_to_csat_ratio")
)
