from pyspark.sql import functions as F

prepared_agents = (
    df_support_agents
    .filter((F.col("tier").isNotNull()) & (F.col("hired_on") >= F.lit("2024-04-14").cast("date")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.initcap(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
    )
)

agent_side = (
    prepared_agents
    .select(
        F.col("agent_name").alias("label"),
        F.col("id").alias("key_id"),
        F.col("hired_on").alias("day"),
        F.lit("agent").alias("record_type"),
    )
)

agent_side_2 = (
    df_support_agents
    .filter(F.length(F.trim(F.col("agent_name"))) >= 4)
    .select(
        F.col("tier").alias("label"),
        F.col("id").alias("key_id"),
        F.col("hired_on").alias("day"),
        F.lit("agent_2").alias("record_type"),
    )
)

result = agent_side.union(agent_side_2)
