from pyspark.sql import functions as F

valid_orders = df_orders.filter(F.col("customer_id").isNotNull())

summary = (
    valid_orders
    .groupBy("status")
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
)

result = (
    summary
    .filter(F.col("mobile_count") > 72)
    .select("status", "max_shipping_fee", "mobile_count")
    .orderBy(F.col("mobile_count").desc())
)
