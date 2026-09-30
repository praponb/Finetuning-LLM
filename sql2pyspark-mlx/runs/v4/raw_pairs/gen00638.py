from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 35, "high").when(F.col("shipping_fee") >= 6, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
    )
    .filter(F.col("completed_count") >= 9)
    .orderBy("first_order_date", F.col("shipping_fee_bucket").desc())
)
