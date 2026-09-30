from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 310)
    .groupBy("order_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
    .filter(F.col("unit_price_count") >= 4)
)
