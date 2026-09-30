from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") < "2024-07-01")
    .select("end_date", F.lit("subscription").alias("source"))
    .unionByName(
        df_shipments
        .select(F.col("shipped_date").alias("end_date"), F.lit("shipment").alias("source"))
    )
    .distinct()
)
