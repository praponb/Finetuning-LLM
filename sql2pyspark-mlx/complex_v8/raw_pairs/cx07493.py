from pyspark.sql import functions as F

prepared_agents = (
    df_support_agents
    .filter((F.col("tier").isin("T2", "T1")) & (F.length(F.trim(F.col("agent_name"))) >= 5))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.length(F.trim(F.col("agent_name"))).alias("agent_name_len"),
    )
)

agent_side = (
    prepared_agents
    .filter(F.col("team").isin("Billing", "VIP", "Onboarding"))
    .select(
        F.col("id").alias("key_id"),
        "tier",
        F.col("hired_on").alias("event_date"),
        F.lit("agent").alias("record_type"),
    )
)

agent_rows = (
    df_support_agents
    .select(
        F.col("id").alias("key_id"),
        "tier",
        F.col("hired_on").alias("event_date"),
        F.lit("agent_2").alias("record_type"),
    )
)

agent_rows_2 = (
    df_support_agents
    .select(
        F.col("id").alias("key_id"),
        F.col("agent_name").alias("tier"),
        F.col("hired_on").alias("event_date"),
        F.lit("agent_3").alias("record_type"),
    )
)

result = (
    agent_side
    .unionByName(agent_rows)
    .unionByName(agent_rows_2)
)
