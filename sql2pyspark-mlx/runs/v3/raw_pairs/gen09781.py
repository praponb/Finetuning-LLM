from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date", F.lit("subscription").alias("source"))
    .union(
        df_shipments
        .filter(F.month("delivered_date").isin([3, 7, 9]))
        .select("delivered_date", F.lit("shipment").alias("source"))
    )
    .distinct()
    .orderBy(F.col("end_date").desc())
)
