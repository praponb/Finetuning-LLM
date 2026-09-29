from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull() & (F.col("shipping_fee") < 24))
    .groupBy(
        F.month("order_date").alias("order_month"),
        F.when(F.col("discount") >= 0.42, "high").when(F.col("discount") >= 0.1, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
)
