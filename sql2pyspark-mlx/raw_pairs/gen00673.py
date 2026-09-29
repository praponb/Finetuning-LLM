from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .select("shipping_fee")
    .union(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select("monthly_fee")
    )
    .distinct()
)
