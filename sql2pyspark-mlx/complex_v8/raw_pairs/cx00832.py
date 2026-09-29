from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter((F.col("team") != "Billing") & (F.upper(F.trim(F.col("agent_name"))).like("H%")))
    .select(
        F.col("id").alias("entity_id"),
        F.col("hired_on").alias("day"),
        F.lit("agent").alias("record_type"),
    )
)

agent_rows_2 = (
    df_support_agents
    .filter(F.col("team").isNotNull())
    .select(
        F.col("id").alias("entity_id"),
        F.col("hired_on").alias("day"),
        F.lit("agent_2").alias("record_type"),
    )
)

result = (
    agent_rows
    .unionByName(agent_rows_2)
    .orderBy("entity_id")
)
