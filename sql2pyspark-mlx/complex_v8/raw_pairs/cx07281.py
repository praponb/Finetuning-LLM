from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter(F.month(F.col("closed_at")).isin(1, 3))
    .select("channel", "priority", F.lit("ticket").alias("source"))
)

agent_side = (
    df_support_agents
    .filter(F.col("tier").isNotNull())
    .select(
        F.col("agent_name").alias("channel"),
        F.col("team").alias("priority"),
        F.lit("agent").alias("source"),
    )
)

result = (
    ticket_rows
    .unionByName(agent_side)
    .orderBy("priority")
)
