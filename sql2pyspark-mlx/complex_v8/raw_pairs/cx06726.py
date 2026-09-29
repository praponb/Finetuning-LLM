from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .select(
        F.lit("ticket").alias("source"),
        F.col("requester_id").alias("key_id"),
        F.col("opened_at").alias("happened_at"),
    )
)

ticket_rows_2 = df_tickets.select(F.lit("ticket_2").alias("source"), "agent_id", "closed_at")

combined = ticket_rows.union(ticket_rows_2)

result = (
    combined
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("key_id").alias("total_key_id"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("source")
)
