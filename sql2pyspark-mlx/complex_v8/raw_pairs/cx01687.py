from pyspark.sql import functions as F

event_side = df_ticket_events.select(F.col("event_type").alias("category"), "event_at")

event_rows = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-03-08 08:00:00").cast("timestamp"))
    .select(F.col("event_type").alias("category"), "event_at")
)

result = (
    event_side
    .unionByName(event_rows)
    .orderBy(F.col("event_at").desc(), "category")
)
