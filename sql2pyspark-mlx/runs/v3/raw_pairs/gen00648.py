from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.month("order_date").isin([3, 5, 12]))

agg = (
    filtered_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
    )
)

result = (
    agg
    .filter(F.col("shipped_count") > 38)
    .orderBy(F.col("shipped_count").desc(), "order_month")
    .limit(10)
)
