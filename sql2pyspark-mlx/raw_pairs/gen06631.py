from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.id"))
    .agg(F.count(F.col("oi.id")).alias("order_item_count"))
)
