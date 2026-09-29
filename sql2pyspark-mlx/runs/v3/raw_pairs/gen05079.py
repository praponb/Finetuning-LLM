from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .subtract(
        df_orders
        .filter(F.col("store_id").isNull())
        .select(F.col("shipping_fee").alias("monthly_fee"))
    )
    .orderBy("monthly_fee")
)
