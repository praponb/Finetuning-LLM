from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("product_id").isin([13, 20, 23, 37]) & (F.col("quantity") >= 6))
)

summary = (
    valid_order_items
    .groupBy("order_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = summary.select("order_id", "unit_price_count")
