from pyspark.sql import functions as F

agent_side = df_support_agents.select("id")

ticket_side = (
    df_tickets
    .filter(F.to_date(F.col("opened_at")) < F.lit("2025-02-06").cast("date"))
    .select(F.col("requester_id").alias("id"))
)

result = agent_side.subtract(ticket_side)
