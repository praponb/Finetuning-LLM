from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .select(
        F.lit("ticket").alias("origin"),
        "requester_id",
        F.col("priority").alias("name"),
    )
)

agent_rows = df_support_agents.select(F.lit("agent").alias("origin"), "id", "tier")

unioned = ticket_rows.union(agent_rows)

result = (
    unioned
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("requester_id").alias("total_requester_id"),
    )
    .orderBy("origin")
)
