from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter(F.col("product_id").isNull() & F.col("unit_price").between(200, 280))
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

agg = (
    filtered_order_items
    .groupBy("product_id")
    .agg(F.count("*").alias("order_item_count"))
)

result = (
    agg
    .orderBy("product_id")
    .limit(5)
)
