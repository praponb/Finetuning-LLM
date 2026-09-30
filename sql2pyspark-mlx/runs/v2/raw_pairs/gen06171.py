from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 17)
    .agg(
        F.max("order_date").alias("latest_order_date"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
    .orderBy("discount_range")
)
