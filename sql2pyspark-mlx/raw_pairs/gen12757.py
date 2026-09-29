from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.lit("subscription").alias("origin"), "monthly_fee")
    .union(
        df_orders
        .filter(F.col("customer_id") == 19)
        .select(F.lit("order").alias("origin"), "shipping_fee")
    )
    .distinct()
)
