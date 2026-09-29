from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status", F.lit("shipment").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") != "enterprise")
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
)
