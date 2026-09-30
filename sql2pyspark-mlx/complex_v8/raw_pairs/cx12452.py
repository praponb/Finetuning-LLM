from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter((F.col("actor").isin("system", "customer")) | (F.col("ticket_id") >= 5))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.when(F.col("event_type").isin("assigned", "reopened"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
    )
)

with_children = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("escalated", "replied")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_clean",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

with_activity = (
    with_children.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
    .filter(F.col("ticket_id").isNotNull())
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("event_event_type_count"),
    )
)

summarized_rows = (
    with_activity.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

with_parent_event = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .join(df_tickets.alias("ti"), F.col("ti.id") == F.col("t.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_clean",
        "s.event_id",
        "s.event_event_type",
        "s.event_actor",
        "s.event_event_at",
        F.col("t.ticket_id").alias("event_ticket_id"),
        F.col("ti.priority").alias("ticket_priority"),
        F.col("ti.opened_at").alias("ticket_opened_at"),
    )
)

labeled = (
    with_parent_event
    .filter(F.col("event_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat(F.col("event_event_type"), F.lit("#"), F.col("event_id").cast("string")).alias("event_event_type_ref"),
        F.round((F.unix_timestamp(F.col("event_event_at")) - F.unix_timestamp(F.col("ticket_opened_at"))) / 60, 1).alias("minutes_ticket_opened_at_to_event_event_at"),
    )
)

combined_details = (
    labeled.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.event_ticket_id"), "inner")
    .filter(F.col("t.agent_id").isNotNull())
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_id",
        "s.event_event_type",
        "s.event_actor",
        "s.event_event_at",
        "s.event_ticket_id",
        "s.ticket_priority",
        "s.event_event_type_ref",
        "s.minutes_ticket_opened_at_to_event_event_at",
        F.col("t.priority").alias("ticket_priority_2"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

result = (
    combined_details
    .filter(~F.col("event_event_type").isin("created", "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_event_type",
        "event_actor",
        "event_event_at",
        "event_ticket_id",
        "ticket_priority",
        "event_event_type_ref",
        "minutes_ticket_opened_at_to_event_event_at",
        "ticket_agent_id",
        "ticket_csat",
        F.length(F.trim(F.col("event_event_type"))).alias("event_event_type_len"),
    )
    .orderBy(F.col("ticket_id").desc())
)
