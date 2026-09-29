from pyspark.sql import functions as F

event_rows = df_ticket_events.select(F.lit("event").alias("record_type"), "id", "ticket_id")

event_side = df_ticket_events.select(F.lit("event_2").alias("record_type"), "id", "ticket_id")

result = (
    event_rows
    .union(event_side)
    .orderBy("record_type", "ticket_id")
)
