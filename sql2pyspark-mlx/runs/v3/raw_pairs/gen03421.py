from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("order_id").isin([11, 12, 20]))
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.avg_quantity") > 6.8)
    .select(F.col("p.id"), F.col("p.brand"), F.col("t.avg_quantity"))
    .orderBy(F.col("id").desc(), F.col("brand").desc())
)
