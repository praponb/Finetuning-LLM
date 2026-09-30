from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("shipping_fee")).otherwise(0)).alias("shipped_shipping_fee"),
    )
    .filter(F.col("returned_count") > 58)
    .orderBy(F.col("shipped_shipping_fee").desc())
)
