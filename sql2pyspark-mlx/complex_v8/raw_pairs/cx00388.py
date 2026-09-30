from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter(F.col("tier") == "T3")
    .select(
        F.lit("agent").alias("record_type"),
        F.col("id").alias("qty"),
        F.col("hired_on").alias("day"),
    )
)

agent_rows = df_support_agents.select(F.lit("agent_2").alias("record_type"), "id", "hired_on")

result = agent_side.union(agent_rows)
