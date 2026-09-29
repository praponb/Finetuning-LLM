from pyspark.sql import functions as F

valid_orders = df_orders.filter(F.col("order_date").between("2023-10-18", "2024-01-01"))

shipping_fee_bucket_summary = (
    valid_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 33, "high").when(F.col("shipping_fee") >= 13, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.sum("discount").alias("total_discount"))
)

result = (
    shipping_fee_bucket_summary
    .orderBy(F.col("shipping_fee_bucket").desc())
    .limit(5)
)
