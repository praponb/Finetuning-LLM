from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        F.col("shipped_date").alias("event_date"),
        F.lit("shipment").alias("source"),
    )
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") > "2024-05-01")
        .select(
        F.col("start_date").alias("event_date"),
        F.lit("subscription").alias("source"),
    )
    )
    .distinct()
    .orderBy(F.col("source").desc())
)
