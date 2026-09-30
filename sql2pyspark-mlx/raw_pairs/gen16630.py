from pyspark.sql import functions as F

result = (
    df_orders
    .rollup(
        F.when(F.col("shipping_fee") >= 44, "high").when(F.col("shipping_fee") >= 15, "mid").otherwise("low").alias("shipping_fee_bucket"),
        "channel",
    )
    .agg(F.min("order_date").alias("min_order_date"))
)
