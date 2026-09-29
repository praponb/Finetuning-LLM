from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") > "2025-06-01") & (F.col("shipping_fee") >= 16))
    .groupBy(
        "channel",
        F.when(F.col("discount") >= 0.31, "high").when(F.col("discount") >= 0.16, "mid").otherwise("low").alias("discount_bucket"),
        "store_id",
    )
    .agg(F.min("order_date").alias("min_order_date"))
)
