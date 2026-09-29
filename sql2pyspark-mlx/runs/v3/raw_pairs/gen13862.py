from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNull())
    .select("shipping_fee")
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id") == 14)
        .select(F.col("monthly_fee").alias("shipping_fee"))
    )
    .distinct()
)
