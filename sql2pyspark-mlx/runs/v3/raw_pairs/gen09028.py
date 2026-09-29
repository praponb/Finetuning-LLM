from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date", F.lit("shipment").alias("source"))
    .union(
        df_subscriptions
        .filter(F.col("end_date") < "2023-10-01")
        .select("end_date", F.lit("subscription").alias("source"))
    )
    .distinct()
)
