from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter(F.col("team").isin("Tech", "Onboarding", "Billing"))
    .select(
        "agent_name",
        F.col("hired_on").alias("day"),
        F.lit("agent").alias("record_type"),
    )
)

agent_side_2 = (
    df_support_agents
    .select(
        F.col("team").alias("agent_name"),
        F.col("hired_on").alias("day"),
        F.lit("agent_2").alias("record_type"),
    )
)

agent_side_3 = (
    df_support_agents
    .select(
        F.col("tier").alias("agent_name"),
        F.col("hired_on").alias("day"),
        F.lit("agent_3").alias("record_type"),
    )
)

all_events = (
    agent_side
    .union(agent_side_2)
    .union(agent_side_3)
)

result = (
    all_events
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.max("day").alias("latest_day"))
    .orderBy("record_type")
)
