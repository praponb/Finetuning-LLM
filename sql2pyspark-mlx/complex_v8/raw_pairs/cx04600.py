from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .select(
        F.col("id").alias("key_id"),
        F.col("agent_name").alias("label"),
        "hired_on",
    )
)

agent_side_2 = (
    df_support_agents
    .select(F.col("id").alias("key_id"), F.col("team").alias("label"), "hired_on")
)

result = (
    agent_side
    .unionByName(agent_side_2)
    .orderBy("hired_on")
)
