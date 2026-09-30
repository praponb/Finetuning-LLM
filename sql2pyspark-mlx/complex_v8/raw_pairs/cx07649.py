from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter(F.col("event_type") == "created")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

with_details_ticket = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.channel").isNotNull())
    .select(
        "s.*",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

line_level_events = (
    with_details_ticket.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor_clean",
        "s.ticket_channel",
        "s.ticket_agent_id",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.min("closed_at").alias("first_ticket_closed_at"))
)

with_counts = (
    line_level_events.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.ticket_agent_id"), "left")
    .select("s.*", "t.first_ticket_closed_at")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"), F.count("*").alias("event_count_2"))
)

with_stats = (
    with_counts.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

scored = (
    with_stats
    .select(
        "*",
        F.round(F.col("event_count_2") / F.nullif(F.col("event_count"), F.lit(0)), 3).alias("event_count_2_per_event_count"),
        F.concat_ws(" / ", F.col("event_event_type"), F.col("actor_clean")).alias("event_event_type_actor_clean_label"),
    )
)

screened = (
    scored.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.ticket_agent_id")) & (F.array_contains(F.col("t.tags"), "refund")) & (F.col("t.opened_at") >= F.lit("2025-01-19 10:00:00").cast("timestamp")), "left_anti")
)

result = (
    screened
    .select(
        "id",
        "ticket_id",
        "ticket_channel",
        "ticket_agent_id",
        "event_id",
        "event_event_at",
        "event_event_type",
        "first_ticket_closed_at",
        "event_count",
        "event_count_2",
        "event_count_2_per_event_count",
    )
    .orderBy(
        "first_ticket_closed_at",
        F.col("event_event_type").desc_nulls_first(),
        "id",
        "event_id",
    )
    .limit(5)
)
