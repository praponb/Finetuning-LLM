from pyspark.sql import functions as F

valid_orders = df_orders.filter(F.col("amount") < 1550)

agg = (
    valid_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 44, "high").when(F.col("shipping_fee") >= 9, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.count("*").alias("order_count"))
)

result = (
    agg
    .filter(F.col("order_count") >= 9)
    .select("shipping_fee_bucket", "order_count")
    .orderBy(F.col("shipping_fee_bucket").desc(), F.col("order_count").desc())
    .limit(3)
)
