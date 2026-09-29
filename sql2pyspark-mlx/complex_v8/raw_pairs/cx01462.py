from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter(F.to_date(F.col("opened_at")) < F.lit("2025-02-12").cast("date"))
    .select("agent_id", "priority")
)

event_rows = df_ticket_events.select("ticket_id", "actor")

result = (
    ticket_rows
    .intersect(event_rows)
    .orderBy(F.col("priority").desc(), "agent_id")
)
