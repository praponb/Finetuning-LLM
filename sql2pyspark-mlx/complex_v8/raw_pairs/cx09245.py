from pyspark.sql import functions as F

scoped = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
    )
)

grouped = (
    scoped
    .groupBy(
        "agent_id",
        F.to_date("closed_at").alias("closed_at_day"),
        F.date_format("opened_at", "yyyy-MM").alias("opened_at_ym"),
    )
    .agg(
        F.sum("requester_id").alias("total_requester_id"),
        F.count("*").alias("cnt"),
    )
)

joined_agent = (
    grouped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.agent_id",
        "s.closed_at_day",
        "s.opened_at_ym",
        "s.cnt",
        F.col("su.tier").alias("agent_tier"),
    )
)

exploded_detail_tickets = (
    joined_agent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "inner")
    .select(
        "s.agent_id",
        "s.closed_at_day",
        "s.opened_at_ym",
        "s.agent_tier",
        F.col("t.id").alias("ticket_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    exploded_detail_tickets
    .filter(F.col("ticket_requester_id") <= 18)
    .select(
        "agent_id",
        "closed_at_day",
        "opened_at_ym",
        "agent_tier",
        "ticket_id",
        F.concat(F.col("agent_tier"), F.lit("#"), F.col("ticket_csat").cast("string")).alias("agent_tier_ref"),
    )
    .orderBy(F.col("opened_at_ym").desc_nulls_first(), "agent_id")
)
