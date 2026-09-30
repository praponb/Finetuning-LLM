from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc(), F.col("id").desc())

cleaned = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "refund")) & (F.col("priority") != "P2"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

with_stats = (
    cleaned.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_event_at")
)

deduped = (
    with_stats
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .rollup("channel", "priority")
    .agg(
        F.grouping("channel").alias("is_total_channel"),
        F.min("closed_at").alias("first_closed_at"),
    )
    .select(
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "is_total_channel",
        "first_closed_at",
    )
)
