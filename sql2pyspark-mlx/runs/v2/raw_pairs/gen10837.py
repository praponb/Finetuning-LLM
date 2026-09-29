from pyspark.sql import functions as F

valid_order_items = df_order_items.filter(F.col("product_id").isNull())

agg = (
    valid_order_items
    .groupBy("product_id")
    .agg(F.count("*").alias("order_item_count"))
)

result = agg.orderBy("order_item_count")
