from pyspark.sql import functions as F

base_agents = (
    df_support_agents
    .filter(F.col("team").isin("Onboarding", "Billing", "VIP"))
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
    )
)

agent_side = (
    base_agents
    .filter((F.col("team").isin("Billing", "VIP", "Tech")) & (F.col("hired_on") < F.lit("2025-02-26").cast("date")))
    .select(
        "tier",
        F.col("hired_on").alias("event_date"),
        F.col("team").alias("label"),
        F.lit("agent").alias("record_type"),
    )
)

agent_side_2 = (
    df_support_agents
    .filter(F.col("hired_on").between(F.lit("2024-05-02").cast("date"), F.lit("2025-03-30").cast("date")))
    .select(
        F.col("team").alias("tier"),
        F.col("hired_on").alias("event_date"),
        F.col("agent_name").alias("label"),
        F.lit("agent_2").alias("record_type"),
    )
)

result = (
    agent_side
    .unionByName(agent_side_2)
    .distinct()
    .orderBy("event_date", "label")
)
