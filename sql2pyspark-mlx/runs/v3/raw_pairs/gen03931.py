from pyspark.sql import functions as F

valid_order_items = df_order_items.filter((F.col("unit_price") > 280) | (F.col("order_id") == 4))

agg = (
    valid_order_items
    .groupBy("product_id", "order_id")
    .agg(F.count("*").alias("order_item_count"))
)

result = agg.orderBy(F.col("product_id").desc())
