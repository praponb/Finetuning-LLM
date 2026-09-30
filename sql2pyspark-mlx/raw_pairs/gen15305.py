from pyspark.sql import functions as F

src = (
    df_order_items
    .filter((F.col("product_id") == 37) & ((F.col("quantity") == 4) | (F.col("order_id") == 20)))
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
)

order_items_stats = (
    src
    .groupBy("product_id")
    .agg(F.count("line_total").alias("line_total_count"))
)

result = order_items_stats
