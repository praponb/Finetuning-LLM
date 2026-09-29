from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("product_id").isin([18, 32, 40]) | F.col("product_id").isNull())
)

order_items_stats = (
    recent_order_items
    .groupBy("product_id", "order_id")
    .agg(F.count("*").alias("num_order_items"))
)

result = order_items_stats.select("product_id", "order_id", "num_order_items")
