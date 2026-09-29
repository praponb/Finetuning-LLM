from pyspark.sql import functions as F

scoped = (
    df_support_agents
    .filter((F.upper(F.trim(F.col("agent_name"))).like("J%")) & (F.col("tier") != "T1"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.lower(F.trim(F.col("team"))).alias("team_clean"),
    )
)

agent_rows = (
    scoped
    .filter(F.col("tier") == "T3")
    .select(F.col("id").alias("key_id"), F.col("hired_on").alias("event_date"))
)

agent_rows_2 = (
    df_support_agents
    .filter((F.col("agent_name").isNotNull()) & (F.col("hired_on") < F.lit("2025-03-30").cast("date")))
    .select("id", "hired_on")
)

result = agent_rows.union(agent_rows_2)
