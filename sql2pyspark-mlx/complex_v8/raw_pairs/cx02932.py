from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_csat = Window.partitionBy("priority", "csat")
w_channel_by_closed_at = Window.partitionBy("channel").orderBy("closed_at", "id")
w_all_by_opened_at_running = Window.orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("agent_id").orderBy(F.col("priority_csat_min_requester_id").desc(), F.col("id").desc())

prepared = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
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

with_windows = (
    prepared
    .select(
        "*",
        F.count(F.lit(1)).over(w_priority_csat).alias("rows_in_priority_csat"),
        F.min("requester_id").over(w_priority_csat).alias("priority_csat_min_requester_id"),
        F.round(F.percent_rank().over(w_channel_by_closed_at), 3).alias("closed_at_percent_rank"),
        F.round(F.col("csat") - F.avg("csat").over(w_priority_csat), 2).alias("csat_vs_priority_csat_avg"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.round(F.col("rows_in_priority_csat") - F.avg("rows_in_priority_csat").over(w_priority_csat), 2).alias("rows_in_priority_csat_vs_priority_csat_avg"),
        F.last("requester_id", ignorenulls=True).over(w_all_by_opened_at_running).alias("last_known_requester_id"),
        F.round(F.col("csat_vs_priority_csat_avg") - F.avg("csat_vs_priority_csat_avg").over(w_priority_csat), 2).alias("csat_vs_priority_csat_avg_vs_priority_csat_avg"),
    )
)

deduped = (
    metrics
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .groupBy("channel", "opened_at")
    .pivot("priority", ["P1", "P2", "P3", "P4"])
    .agg(F.sum("last_known_requester_id"))
    .orderBy("channel", "opened_at")
)
