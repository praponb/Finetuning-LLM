from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 37, "high").when(F.col("shipping_fee") >= 11, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "store", F.col("amount")).otherwise(0)).alias("store_amount"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("shipping_fee")).otherwise(0)).alias("cancelled_shipping_fee"),
    )
    .orderBy(F.col("store_amount").desc())
)
