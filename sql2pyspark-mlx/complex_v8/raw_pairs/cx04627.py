from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter((F.col("requester_id") >= 3) & (F.to_date(F.col("opened_at")) < F.lit("2025-03-02").cast("date")))
    .select(
        F.col("priority").alias("category"),
        F.col("opened_at").alias("happened_at"),
        F.col("channel").alias("category_2"),
    )
)

ticket_side = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) & (F.col("priority").isin("P1", "P2")))
    .select("priority", "closed_at", "channel")
)

event_rows = df_ticket_events.select("actor", "event_at", "event_type")

result = (
    ticket_rows
    .union(ticket_side)
    .union(event_rows)
    .orderBy(F.col("happened_at").desc())
)
