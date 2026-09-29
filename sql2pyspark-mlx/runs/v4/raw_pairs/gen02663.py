from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 30)
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
        F.min("order_date").alias("min_order_date"),
    )
    .filter(F.col("shipped_count") < 5)
    .select("channel", "shipped_count", "min_order_date")
    .orderBy(F.col("channel").desc(), F.col("shipped_count").desc())
    .limit(1)
)
