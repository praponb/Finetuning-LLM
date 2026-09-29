from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.col("hired_on") < F.lit("2024-12-31").cast("date")) & (F.upper(F.trim(F.col("agent_name"))).like("G%")))
    .select(
        F.col("id").alias("entity_id"),
        F.col("tier").alias("category"),
        "hired_on",
        F.lit("agent").alias("src"),
    )
)

agent_rows = (
    df_support_agents
    .filter((F.lower(F.col("agent_name")).like("%jo%")) & (F.col("hired_on") < F.lit("2025-03-30").cast("date")))
    .select(
        F.col("id").alias("entity_id"),
        F.col("agent_name").alias("category"),
        "hired_on",
        F.lit("agent_2").alias("src"),
    )
)

result = (
    agent_side
    .union(agent_rows)
    .distinct()
    .orderBy("src", "hired_on")
)
