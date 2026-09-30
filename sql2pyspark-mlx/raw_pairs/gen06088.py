from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").isNotNull())
    .select(F.col("start_date").alias("event_date"))
    .subtract(
        df_orders
        .filter(F.col("order_date").isNotNull())
        .select(F.col("order_date").alias("event_date"))
    )
)
