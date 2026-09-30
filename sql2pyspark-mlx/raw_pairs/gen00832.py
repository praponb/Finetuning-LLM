from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("p.stock_qty") == 217)
    .groupBy(F.col("p.id"), F.col("p.product_name"))
    .agg(F.count(F.col("oi.id")).alias("order_item_count"))
)
