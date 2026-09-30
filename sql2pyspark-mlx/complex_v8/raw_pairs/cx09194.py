from pyspark.sql import functions as F

event_rows = df_ticket_events.select("id", F.col("ticket_id").alias("key_id"), "actor")

ticket_side = (
    df_tickets
    .filter(F.to_date(F.col("opened_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        F.col("agent_id").alias("id"),
        F.col("csat").alias("key_id"),
        F.col("priority").alias("actor"),
    )
)

result = event_rows.unionByName(ticket_side)
