from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("last_restock") < "2023-10-01")
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.product_name"), F.col("t.total_quantity"))
)
