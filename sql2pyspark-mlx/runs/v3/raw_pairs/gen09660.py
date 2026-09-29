from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([2, 27, 30, 37]))
    .select("status", F.lit("shipment").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee").between(200, 210))
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy("status")
)
