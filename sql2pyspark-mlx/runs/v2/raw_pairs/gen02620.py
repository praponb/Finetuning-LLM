from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") < "2025-08-25") & (F.col("store_id") == 1))
    .agg(
        F.countDistinct("shipping_fee").alias("unique_channels"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.sum("amount").alias("total_amount"),
    )
)
