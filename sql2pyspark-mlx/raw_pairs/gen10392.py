from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([2, 5, 8]))
    .select(F.lit("shipment").alias("source"), "shipped_date")
    .union(
        df_subscriptions
        .select(F.lit("subscription").alias("source"), "start_date")
    )
    .distinct()
    .orderBy(F.col("shipped_date").desc())
)
