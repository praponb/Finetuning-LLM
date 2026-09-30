from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter((F.col("event_type").isin("resolved", "replied")) & (F.col("ticket_id").isNotNull()))
    .select(F.col("id").alias("key_id"), "ticket_id", F.lit("event").alias("src"))
)

event_side_2 = df_ticket_events.select("ticket_id", "id", F.lit("event_2").alias("src"))

result = event_side.union(event_side_2)
