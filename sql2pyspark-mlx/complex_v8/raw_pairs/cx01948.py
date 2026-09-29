from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

with_parent_2 = (
    with_parent.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") <= 4), "inner")
    .filter(F.size(F.col("t.tags")) >= 1)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.ticket_closed_at",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("event_type").isin("reopened", "assigned"), 1)).alias("event_event_type_count"),
    )
)

with_stats = (
    with_parent_2.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

enriched = (
    with_stats
    .select(
        "*",
        (F.col("ticket_closed_at") + F.expr("INTERVAL 15 MINUTES")).alias("ticket_closed_at_plus_15m"),
        F.least(F.col("event_event_type_count"), F.col("event_count")).alias("least_event_event_type_count_event_count"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

profiled = (
    enriched.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at")
)

with_parent_3 = (
    profiled.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.ticket_closed_at",
        "s.ticket_tags",
        "s.event_count",
        "s.ticket_closed_at_plus_15m",
        "s.least_event_event_type_count_event_count",
        "s.first_event_at",
        F.col("t.tags").alias("ticket_tags_2"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    with_parent_3
    .select(
        "id",
        "event_type",
        "ticket_closed_at",
        "ticket_tags",
        "event_count",
        "least_event_event_type_count_event_count",
        "ticket_tags_2",
        "ticket_csat",
        "ticket_requester_id",
        F.date_trunc("hour", F.col("ticket_closed_at")).alias("ticket_closed_at_hour"),
    )
    .orderBy(F.col("ticket_closed_at_hour").desc())
)
