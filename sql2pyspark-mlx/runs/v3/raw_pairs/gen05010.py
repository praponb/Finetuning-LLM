from pyspark.sql import functions as F

result = (
    df_order_items
    .select(F.col("unit_price").alias("value"))
    .unionByName(df_subscriptions.select(F.col("monthly_fee").alias("value")))
    .distinct()
)
