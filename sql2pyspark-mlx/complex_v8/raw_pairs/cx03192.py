from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter((~F.col("priority").isin("P1", "P3")) & (F.col("opened_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")))
    .select(
        F.col("agent_id").alias("ref_id"),
        F.col("id").alias("key_id"),
        F.lit("ticket").alias("record_type"),
    )
)

ticket_side_2 = (
    df_tickets
    .filter(F.col("channel").isin("phone", "chat", "web"))
    .select(
        F.col("agent_id").alias("ref_id"),
        F.col("csat").alias("key_id"),
        F.lit("ticket_2").alias("record_type"),
    )
)

all_events = ticket_side.union(ticket_side_2)

result = (
    all_events
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("key_id").alias("total_key_id"))
    .orderBy("record_type")
)
