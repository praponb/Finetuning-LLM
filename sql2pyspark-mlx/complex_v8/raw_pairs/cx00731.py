from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 43) & (F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(F.col("actor") == "customer", 1)).alias("event_actor_count"),
    )
)

profiled = (
    base_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

combined_details = (
    profiled.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.actor_slug",
        "s.last_event_at",
        "s.event_actor_count",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_slug",
        "last_event_at",
        "event_actor_count",
        "ticket_priority",
        "ticket_opened_at",
    )
    .orderBy("ticket_priority", "id")
    .limit(10)
)
