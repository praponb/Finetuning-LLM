from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2023-07-01")
    .select("status", F.lit("shipment").alias("record_type"))
    .unionByName(
        df_subscriptions
        .select("status", F.lit("subscription").alias("record_type"))
    )
    .distinct()
)
