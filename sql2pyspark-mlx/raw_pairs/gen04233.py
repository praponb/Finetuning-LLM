from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("quantity").between(4, 6) & F.col("product_id").isNull())
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

order_items_stats = (
    recent_order_items
    .groupBy("product_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    order_items_stats
    .filter(F.col("row_count") > 8)
    .select("product_id", "row_count")
)
