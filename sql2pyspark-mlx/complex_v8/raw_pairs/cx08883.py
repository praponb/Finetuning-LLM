from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.col("priority") == "P2")
    .select("requester_id")
)

agent_side = (
    df_support_agents
    .filter(F.col("hired_on") < F.lit("2025-02-26").cast("date"))
    .select(F.col("id").alias("requester_id"))
)

result = (
    ticket_side
    .subtract(agent_side)
    .orderBy(F.col("requester_id").desc())
)
