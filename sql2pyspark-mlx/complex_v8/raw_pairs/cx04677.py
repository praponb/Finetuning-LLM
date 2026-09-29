from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter(F.col("tier").isin("T3", "T1"))
    .select("team", F.col("id").alias("ref_id"), F.lit("agent").alias("src"))
)

agent_rows = (
    df_support_agents
    .select(
        F.col("tier").alias("team"),
        F.col("id").alias("ref_id"),
        F.lit("agent_2").alias("src"),
    )
)

unioned = (
    agent_side
    .unionByName(agent_rows)
    .distinct()
)

result = (
    unioned
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("src")
)
