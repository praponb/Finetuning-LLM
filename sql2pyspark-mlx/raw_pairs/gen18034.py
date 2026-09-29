from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .select("shipping_fee", F.lit("order").alias("origin"))
    .union(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select("monthly_fee", F.lit("subscription").alias("origin"))
    )
)
