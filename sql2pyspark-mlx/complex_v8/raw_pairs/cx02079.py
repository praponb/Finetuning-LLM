from pyspark.sql import functions as F

prepared_agents = (
    df_support_agents
    .filter((F.upper(F.trim(F.col("agent_name"))).like("K%")) & (F.col("team").isin("VIP", "Billing")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.concat_ws(" / ", F.col("tier"), F.col("team")).alias("tier_team_label"),
    )
)

agent_side = prepared_agents.select("tier", "hired_on", F.lit("agent").alias("source"))

agent_side_2 = (
    df_support_agents
    .filter(F.col("team").isin("Tech", "Onboarding"))
    .select("team", "hired_on", F.lit("agent_2").alias("source"))
)

unioned = (
    agent_side
    .union(agent_side_2)
    .distinct()
)

result = (
    unioned
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("hired_on").alias("latest_hired_on"),
    )
    .orderBy("source")
)
