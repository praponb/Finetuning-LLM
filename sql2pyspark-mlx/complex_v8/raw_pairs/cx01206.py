from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter((F.col("team") == "VIP") & (F.lower(F.col("agent_name")).like("%fa%")))
    .select(
        F.lit("agent").alias("record_type"),
        "tier",
        "hired_on",
        F.col("agent_name").alias("category"),
    )
)

agent_side = (
    df_support_agents
    .filter(F.col("tier") == "T1")
    .select(
        F.lit("agent_2").alias("record_type"),
        F.col("team").alias("tier"),
        "hired_on",
        F.col("agent_name").alias("category"),
    )
)

agent_rows_2 = (
    df_support_agents
    .select(
        F.lit("agent_3").alias("record_type"),
        F.col("team").alias("tier"),
        "hired_on",
        F.col("tier").alias("category"),
    )
)

all_events = (
    agent_rows
    .unionByName(agent_side)
    .unionByName(agent_rows_2)
)

result = (
    all_events
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("hired_on").alias("latest_hired_on"),
    )
    .orderBy("record_type")
)
