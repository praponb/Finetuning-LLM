from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .filter(F.col("event_type") == "assigned")
    .select("ticket_id", "id")
)

event_rows_2 = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 55) & (F.col("event_type").isNotNull()))
    .select("ticket_id", "id")
)

result = (
    event_rows
    .unionByName(event_rows_2)
    .distinct()
    .orderBy(F.col("ticket_id").desc(), "id")
)
