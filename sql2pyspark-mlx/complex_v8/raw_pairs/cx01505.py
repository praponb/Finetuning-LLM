from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter((F.col("actor") == "customer") & (F.hour(F.col("event_at")).between(8, 10)))
    .select(F.col("id").alias("key_id"), F.col("event_at").alias("happened_at"))
)

event_rows = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 7) & (F.month(F.col("event_at")).isin(1, 2)))
    .select(F.col("id").alias("key_id"), F.col("event_at").alias("happened_at"))
)

unioned = (
    event_side
    .unionByName(event_rows)
    .distinct()
)

result = (
    unioned
    .groupBy("key_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("key_id")
)
