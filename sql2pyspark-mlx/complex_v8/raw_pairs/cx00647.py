from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter(F.col("hired_on") < F.lit("2025-02-26").cast("date"))
    .select(F.col("id").alias("ref_id"), F.col("team").alias("label"))
)

agent_side = df_support_agents.select("id", "team")

ticket_rows = (
    df_tickets
    .filter((F.col("csat") > 3) & (F.col("opened_at") < F.lit("2025-02-18 13:00:00").cast("timestamp")))
    .select("agent_id", "priority")
)

result = (
    agent_rows
    .union(agent_side)
    .union(ticket_rows)
)
