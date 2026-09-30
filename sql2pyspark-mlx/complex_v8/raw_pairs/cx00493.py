from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .select(
        "channel",
        F.col("id").alias("entity_id"),
        F.lit("ticket").alias("origin"),
    )
)

agent_side = (
    df_support_agents
    .filter(F.col("tier") == "T1")
    .select(
        F.col("tier").alias("channel"),
        F.col("id").alias("entity_id"),
        F.lit("agent").alias("origin"),
    )
)

result = ticket_rows.union(agent_side)
