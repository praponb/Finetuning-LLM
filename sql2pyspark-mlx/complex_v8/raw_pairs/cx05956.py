from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .filter((F.col("actor").isin("system", "customer")) & (F.col("ticket_id").isNotNull()))
    .select(F.col("ticket_id").alias("key_id"))
)

event_rows_2 = (
    df_ticket_events
    .filter((~F.col("event_type").isin("assigned", "resolved")) & (F.col("actor").isin("system", "customer")))
    .select(F.col("id").alias("key_id"))
)

result = (
    event_rows
    .intersect(event_rows_2)
    .orderBy(F.col("key_id").desc())
)
