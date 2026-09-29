from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter((F.col("event_type").isNotNull()) & (F.col("ticket_id") <= 16))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_side = prepared.select("id", F.col("ticket_id").alias("key_id"))

event_side_2 = (
    df_ticket_events
    .filter(F.col("event_type") == "resolved")
    .select("id", F.col("ticket_id").alias("key_id"))
)

combined = event_side.union(event_side_2)

result = (
    combined
    .groupBy("id")
    .agg(F.count("*").alias("n_rows"), F.sum("key_id").alias("total_key_id"))
    .orderBy("id")
)
