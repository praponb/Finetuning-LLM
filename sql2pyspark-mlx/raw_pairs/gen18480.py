from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("amount") > 1250)
    .withColumn("amount_minus_discount", (F.col("amount") - F.col("discount")))
)

orders_stats = (
    recent_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 42, "high").when(F.col("shipping_fee") >= 19, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.count("*").alias("cnt"),
    )
)

result = (
    orders_stats
    .filter(F.col("cnt") < 5)
    .select("shipping_fee_bucket", "cnt")
    .orderBy(F.col("cnt").desc())
)
