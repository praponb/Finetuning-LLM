from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-19 08:00:00").cast("timestamp")) & (F.col("actor").isin("system", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_counts = (
    scoped_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

exploded_detail = (
    with_counts.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_actor_label",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "event_at",
        "event_type",
        "event_id",
        "event_event_at",
        "event_event_type",
        F.when(F.col("event_event_type").isin("created", "resolved"), "group_a").when(F.col("event_event_type") == "escalated", "group_b").otherwise("other").alias("event_event_type_group"),
    )
)
