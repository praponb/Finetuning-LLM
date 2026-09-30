from pyspark.sql import functions as F

filtered_agents = (
    df_support_agents
    .filter((F.col("hired_on").between(F.lit("2024-07-09").cast("date"), F.lit("2025-02-26").cast("date"))) & (F.col("team").isNotNull()))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.concat(F.col("team"), F.lit("#"), F.col("id").cast("string")).alias("team_ref"),
        F.concat(F.col("tier"), F.lit("#"), F.col("id").cast("string")).alias("tier_ref"),
    )
)

scored = (
    filtered_agents
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        "team_ref",
        "tier_ref",
        F.when(F.col("team").isin("Billing", "Tech"), "group_a").when(F.col("team") == "VIP", "group_b").otherwise("other").alias("team_group"),
    )
)

with_children = (
    scored.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_name",
        "s.tier",
        "s.tier_ref",
        "s.team_group",
        F.col("t.id").alias("ticket_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    with_children
    .groupBy("ticket_opened_at")
    .pivot("team_group", ["other", "group_a"])
    .agg(F.count("ticket_requester_id"))
    .orderBy("ticket_opened_at")
)
