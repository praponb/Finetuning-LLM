from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter(F.col("channel") == "phone")
    .select("csat", "agent_id", "opened_at")
)

event_rows = (
    df_ticket_events
    .select(
        F.col("id").alias("csat"),
        F.col("ticket_id").alias("agent_id"),
        F.col("event_at").alias("opened_at"),
    )
)

result = (
    ticket_rows
    .union(event_rows)
    .distinct()
    .orderBy(F.col("agent_id").desc(), "opened_at")
)
