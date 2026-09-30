from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("status") == "COMPLETED") & (F.col("order_date") >= "2023-04-01")) | F.col("shipping_fee").between(12, 24))
    .groupBy("store_id", "status")
    .agg(
        F.count("shipping_fee").alias("shipping_fee_count"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
    )
    .filter(F.col("store_count") >= 67)
)
