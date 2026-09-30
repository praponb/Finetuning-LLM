from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2024-06-26", "2024-09-01"))
    .groupBy("store_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
    .filter(F.col("min_shipping_fee") > 27)
    .drop("min_shipping_fee")
)
