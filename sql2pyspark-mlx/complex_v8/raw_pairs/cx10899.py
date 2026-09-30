from pyspark.sql import functions as F

scoped = (
    df_support_agents
    .filter(~F.col("team").isin("Onboarding", "VIP"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.when(F.col("tier").isin("T1", "T3"), "group_a").when(F.col("tier") == "T2", "group_b").otherwise("other").alias("tier_group"),
    )
)

agent_side = (
    scoped
    .filter(F.col("tier_group").isin("other", "group_b"))
    .select(F.col("id").alias("ref_id"))
)

agent_side_2 = df_support_agents.select("id")

result = (
    agent_side
    .subtract(agent_side_2)
    .orderBy("ref_id")
)
