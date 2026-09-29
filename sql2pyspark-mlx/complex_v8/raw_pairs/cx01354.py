from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-03-14").cast("date")) & (F.col("ticket_id") >= 38))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_details = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

scored = (
    with_details
    .select(
        "*",
        F.array_join(F.array_sort(F.col("ticket_tags")), ",").alias("ticket_tags_text"),
    )
)

exploded = (
    scored
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_priority",
        "ticket_opened_at",
        "agent_hired_on",
        "ticket_tags_text",
        F.explode_outer("ticket_tags").alias("ticket_tag"),
    )
)

filtered = (
    exploded.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("resolved", "escalated", "replied")), "left_anti")
)

result = (
    filtered
    .select("ticket_priority", "ticket_tag")
    .distinct()
    .orderBy(F.col("ticket_tag").desc(), F.col("ticket_priority").desc())
)
