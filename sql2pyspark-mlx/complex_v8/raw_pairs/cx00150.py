from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-03-04 11:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

joined_agent = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isin("Onboarding", "Tech")), "inner")
    .filter(F.col("su.team").isNotNull())
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.csat",
        F.col("su.team").alias("agent_team"),
    )
)

with_details_agent = (
    joined_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.agent_team",
        F.col("su.team").alias("agent_team_2"),
    )
)

without_matches = (
    with_details_agent
    .crossJoin(with_details_agent.agg(F.avg("csat").alias("overall_avg_csat")))
    .filter(F.col("csat") > F.col("overall_avg_csat"))
    .drop("overall_avg_csat")
)

result = (
    without_matches
    .groupBy("agent_team")
    .pivot("agent_team_2", ["Tech", "VIP", "Billing"])
    .agg(F.count("csat"))
)
