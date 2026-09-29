from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.avg_quantity") > 160)
    .select(F.col("p.id"), F.col("p.price"), F.col("t.avg_quantity"))
)
