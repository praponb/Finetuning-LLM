from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter((F.col("requester_id") >= 8) & (F.col("closed_at") < F.lit("2025-02-26 16:00:00").cast("timestamp")))
    .select(F.col("csat").alias("key_id"), F.col("channel").alias("label"))
)

agent_side = df_support_agents.select("id", "team")

result = (
    ticket_rows
    .union(agent_side)
    .distinct()
    .orderBy("label")
)
