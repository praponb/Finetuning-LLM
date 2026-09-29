from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date", F.lit("shipment").alias("source"))
    .unionByName(
        df_subscriptions
        .select(
        F.col("end_date").alias("shipped_date"),
        F.lit("subscription").alias("source"),
    )
    )
    .orderBy("shipped_date")
)
