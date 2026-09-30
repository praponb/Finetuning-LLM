from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter((F.col("event_type").isNotNull()) & (F.hour(F.col("event_at")).between(8, 10)))
    .select("actor")
)

event_side_2 = df_ticket_events.select("event_type")

result = event_side.subtract(event_side_2)
