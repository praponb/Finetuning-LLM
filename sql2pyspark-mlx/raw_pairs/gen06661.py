from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
    .withColumn("line_total_2", (F.col("unit_price") - F.col("quantity")))
)

result = (
    filtered
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.quantity")).alias("min_quantity"),
        F.max(F.col("src.line_total_2")).alias("max_line_total_2"),
    )
)
