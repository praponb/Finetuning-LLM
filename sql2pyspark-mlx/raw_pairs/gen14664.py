from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") >= 2023)
    .select(F.lit("shipment").alias("source"), "status")
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id").isin([8, 12, 17, 34]))
        .select(F.lit("subscription").alias("source"), "status")
    )
    .distinct()
)
