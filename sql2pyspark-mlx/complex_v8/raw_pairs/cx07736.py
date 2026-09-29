from pyspark.sql import functions as F

recent = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.when(F.col("tier").isin("T1", "T2"), "group_a").when(F.col("tier") == "T3", "group_b").otherwise("other").alias("tier_group"),
    )
)

agent_rows = (
    recent
    .filter(F.year(F.col("hired_on")) == 2024)
    .select(F.col("agent_name").alias("name"), "hired_on")
)

agent_side = (
    df_support_agents
    .filter((~F.col("tier").isin("T1", "T3")) & (F.col("hired_on") >= F.lit("2024-07-28").cast("date")))
    .select("team", "hired_on")
)

result = (
    agent_rows
    .union(agent_side)
    .orderBy("name", "hired_on")
)
