from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .select(
        F.lit("agent").alias("record_type"),
        F.col("id").alias("entity_id"),
        "hired_on",
    )
)

agent_side = (
    df_support_agents
    .filter(F.length(F.trim(F.col("agent_name"))) >= 8)
    .select(F.lit("agent_2").alias("record_type"), "id", "hired_on")
)

unioned = agent_rows.union(agent_side)

result = (
    unioned
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
        F.max("hired_on").alias("latest_hired_on"),
    )
    .orderBy("record_type")
)
