from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee", F.lit("order").alias("source"))
    .unionByName(
        df_subscriptions
        .select(
        F.col("monthly_fee").alias("shipping_fee"),
        F.lit("subscription").alias("source"),
    )
    )
    .distinct()
)
