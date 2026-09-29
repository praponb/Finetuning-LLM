from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 15)
    .groupBy("status")
    .agg(
        F.count("shipping_fee").alias("shipping_fee_count"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
    .filter(F.col("cancelled_count") < 31)
    .drop("cancelled_count")
    .orderBy("status", F.col("shipping_fee_count").desc())
    .limit(5)
)
