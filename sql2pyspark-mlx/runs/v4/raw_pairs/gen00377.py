from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2023-05-01")
    .select("status", F.lit("shipment").alias("source"))
    .unionByName(
        df_subscriptions
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
)
