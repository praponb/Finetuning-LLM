from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("customer_id").isNotNull() | F.col("store_id").isNull())
    .withColumn("amount_with_shipping", (F.col("shipping_fee") + F.col("amount")))
)

summary = (
    filtered_orders
    .groupBy("channel", "status")
    .agg(F.count("discount").alias("discount_count"))
)

result = (
    summary
    .filter(F.col("discount_count") < 9)
    .orderBy("status", "channel")
    .limit(5)
)
