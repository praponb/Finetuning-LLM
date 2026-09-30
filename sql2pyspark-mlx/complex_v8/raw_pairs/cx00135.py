from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter((F.month(F.col("closed_at")).isin(1, 3)) & (F.array_contains(F.col("tags"), "login")))
    .select(
        F.lit("ticket").alias("src"),
        "id",
        "opened_at",
        "channel",
    )
)

ticket_side_2 = (
    df_tickets
    .select(
        F.lit("ticket_2").alias("src"),
        "agent_id",
        "opened_at",
        "channel",
    )
)

unioned = ticket_side.union(ticket_side_2)

result = (
    unioned
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("opened_at").alias("latest_opened_at"),
    )
    .orderBy("src")
)
