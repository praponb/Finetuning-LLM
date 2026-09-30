from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.lit("shipment").alias("record_type"), "status")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan").isin(["basic", "premium", "enterprise"]))
        .select(F.lit("subscription").alias("record_type"), "status")
    )
    .distinct()
)
