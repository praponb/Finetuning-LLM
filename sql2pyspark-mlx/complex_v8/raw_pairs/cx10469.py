from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter((F.col("closed_at") >= F.lit("2025-02-02 03:00:00").cast("timestamp")) & (F.col("channel").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
    )
)

ticket_side = (
    scoped_tickets
    .select(F.col("csat").alias("ref_id"), F.col("channel_ref").alias("category"))
)

ticket_rows = df_tickets.select("requester_id", "priority")

combined = ticket_side.union(ticket_rows)

result = (
    combined
    .groupBy("category")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("category")
)
