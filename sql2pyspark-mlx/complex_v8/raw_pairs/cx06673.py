from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("csat").alias("max_ticket_csat"),
        F.min("csat").alias("min_ticket_csat"),
        F.count("*").alias("ticket_count"),
    )
)

with_counts = (
    valid_tickets.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.max_ticket_csat",
        "t.min_ticket_csat",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

combined_details = (
    with_counts.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier") == "T3")
    .select("s.*", F.col("su.tier").alias("agent_tier"))
)

summary = (
    combined_details
    .filter(F.col("min_ticket_csat") >= 2)
    .groupBy("ticket_count")
    .agg(
        F.count(F.when(F.col("agent_tier").isin("T1", "T2"), 1)).alias("n_agent_tier_in_t1_t2"),
        F.count("*").alias("row_count"),
        F.max("opened_at").alias("last_opened_at"),
        F.sum("ticket_count").alias("total_ticket_count"),
    )
)

result = (
    summary
    .filter(F.col("n_agent_tier_in_t1_t2") >= 2)
    .select(
        "ticket_count",
        "n_agent_tier_in_t1_t2",
        "row_count",
        "last_opened_at",
        "total_ticket_count",
    )
    .orderBy(F.col("ticket_count").desc())
)
