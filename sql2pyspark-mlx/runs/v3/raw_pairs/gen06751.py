from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-11-10", "2025-01-18"))
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.max("order_date").alias("latest_order_date"),
    )
    .filter(F.col("store_count") >= 3)
    .orderBy("latest_order_date", "order_month")
    .limit(20)
)
