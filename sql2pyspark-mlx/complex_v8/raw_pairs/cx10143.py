from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("replied", "escalated"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

with_details_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.size(F.col("t.tags")) >= 1)
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

detail_rows_events = (
    with_details_ticket.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_group",
        "s.actor_clean",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    detail_rows_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_clean",
        "event_id",
        "event_actor",
        "event_event_type",
        F.concat(F.col("event_type_group"), F.lit("#"), F.col("event_id").cast("string")).alias("event_type_group_ref"),
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
    )
    .orderBy(
        F.col("actor_clean").asc_nulls_last(),
        F.col("event_id").asc_nulls_last(),
    )
)
