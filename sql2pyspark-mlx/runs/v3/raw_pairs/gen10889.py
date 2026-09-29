from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.row_count") > 6)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("t.row_count"),
    )
    .orderBy("category", F.col("id").desc())
)
