from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

totals = (
    filtered
    .groupBy("actor", "ticket_id")
    .agg(
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.round(100 * F.count(F.when(F.col("actor") == "system", 1)) / F.count("*"), 1).alias("pct_actor_system"),
    )
)

joined = (
    totals.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.actor",
        "s.ticket_id",
        "s.distinct_ticket_ids",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

with_parent_ticket = (
    joined.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (~F.col("t.priority").isin("P4", "P3")), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.actor",
        "s.ticket_id",
        "s.distinct_ticket_ids",
        "s.ticket_opened_at",
        "s.ticket_csat",
        "s.ticket_agent_id",
        F.col("t.opened_at").alias("ticket_opened_at_2"),
        F.col("t.channel").alias("ticket_channel"),
        "su.agent_name",
    )
)

enriched_agent = (
    with_parent_ticket.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.ticket_agent_id"), "inner")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.actor",
        "s.ticket_id",
        "s.distinct_ticket_ids",
        "s.ticket_opened_at",
        "s.ticket_agent_id",
        "s.ticket_opened_at_2",
        "s.ticket_channel",
        "s.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.agent_name").alias("agent_agent_name"),
        F.col("t.csat").alias("ticket_csat_2"),
        F.col("t.opened_at").alias("ticket_opened_at_3"),
    )
)

result = (
    enriched_agent
    .rollup("ticket_id", "ticket_channel", "ticket_agent_id")
    .agg(F.countDistinct("ticket_id").alias("distinct_ticket_ids_2"))
    .select(
        "ticket_id",
        F.coalesce(F.col("ticket_channel"), F.lit("ALL")).alias("ticket_channel"),
        "ticket_agent_id",
        "distinct_ticket_ids_2",
    )
)
