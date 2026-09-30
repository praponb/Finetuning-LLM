from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .select(F.col("shipping_fee").alias("value"))
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select(F.col("monthly_fee").alias("value"))
    )
    .distinct()
)
