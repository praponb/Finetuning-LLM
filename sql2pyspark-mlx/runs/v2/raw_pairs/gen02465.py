from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNotNull() | F.col("order_date").between("2024-06-01", "2024-07-01"))
    .groupBy(
        F.when(F.col("shipping_fee") >= 36, "high").when(F.col("shipping_fee") >= 17, "mid").otherwise("low").alias("shipping_fee_bucket"),
        "status",
    )
    .agg(F.sum("discount").alias("total_discount"))
)
