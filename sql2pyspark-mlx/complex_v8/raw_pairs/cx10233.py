from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type").orderBy("last_event_at", "id")

scoped = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
    )
)

with_stats = (
    scoped.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at", "t.first_event_at")
)

latest_per_key = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined = (
    latest_per_key.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(~F.array_contains(F.col("t.tags"), "upgrade"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_prefix",
        "s.event_type_ref",
        "s.last_event_at",
        "s.first_event_at",
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    joined
    .filter(F.col("event_at") < F.lit("2025-02-24 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_prefix",
        "event_type_ref",
        "last_event_at",
        "first_event_at",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
    .orderBy(F.col("first_event_at").desc(), F.col("event_type_ref").desc())
)
