from pyspark.sql import functions as F

valid_agents = df_support_agents.filter(F.col("team") == "Billing")

agent_rows = (
    valid_agents
    .filter(F.col("hired_on").isNotNull())
    .select(
        "team",
        F.col("agent_name").alias("category"),
        F.col("hired_on").alias("event_date"),
    )
)

agent_rows_2 = (
    df_support_agents
    .filter((F.upper(F.trim(F.col("agent_name"))).like("H%")) & (~F.col("team").isin("Billing", "VIP")))
    .select(
        F.col("tier").alias("team"),
        F.col("agent_name").alias("category"),
        F.col("hired_on").alias("event_date"),
    )
)

agent_rows_3 = (
    df_support_agents
    .filter((~F.col("tier").isin("T2", "T1")) & (F.col("team").isNotNull()))
    .select(
        "team",
        F.col("agent_name").alias("category"),
        F.col("hired_on").alias("event_date"),
    )
)

result = (
    agent_rows
    .unionByName(agent_rows_2)
    .unionByName(agent_rows_3)
    .orderBy("event_date")
)
