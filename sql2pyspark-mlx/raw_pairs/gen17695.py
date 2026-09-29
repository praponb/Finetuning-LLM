from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").isNotNull())
    .select(F.col("start_date").alias("event_date"))
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date").isNotNull())
        .select(F.col("delivered_date").alias("event_date"))
    )
    .orderBy("event_date")
)
