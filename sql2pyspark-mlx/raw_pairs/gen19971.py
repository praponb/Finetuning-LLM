from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date", F.lit("shipment").alias("kind"))
    .union(
        df_subscriptions
        .select("start_date", F.lit("subscription").alias("kind"))
    )
    .distinct()
    .orderBy("delivered_date")
)
