from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date", F.lit("subscription").alias("source"))
    .unionByName(
        df_shipments
        .select(
        F.col("delivered_date").alias("start_date"),
        F.lit("shipment").alias("source"),
    )
    )
)
