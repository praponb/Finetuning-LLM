from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

labeled = (
    enriched_ticket
    .filter(F.col("event_at") < F.lit("2025-02-12 13:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "ticket_opened_at",
        (F.col("ticket_opened_at") + F.expr("INTERVAL 15 MINUTES")).alias("ticket_opened_at_plus_15m"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

annotated = (
    labeled
    .select(
        "*",
        F.length(F.trim(F.col("actor_clean"))).alias("actor_clean_len"),
        F.when(F.col("event_type").isin("resolved", "assigned"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
    )
)

enriched_ticket_2 = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select("s.*", F.col("t.tags").alias("ticket_tags"), "su.agent_name")
)

result = (
    enriched_ticket_2
    .select(
        "id",
        "ticket_id",
        "event_type",
        "ticket_opened_at",
        "ticket_opened_at_plus_15m",
        "actor_clean",
        "actor_clean_len",
        "event_type_group",
        "ticket_tags",
        "agent_name",
        (F.col("ticket_opened_at") + F.expr("INTERVAL 15 MINUTES")).alias("ticket_opened_at_plus_15m_2"),
        F.to_date(F.col("ticket_opened_at")).alias("ticket_opened_at_date"),
    )
    .orderBy(
        F.col("ticket_opened_at_date").asc_nulls_last(),
        F.col("ticket_id").desc(),
    )
)
