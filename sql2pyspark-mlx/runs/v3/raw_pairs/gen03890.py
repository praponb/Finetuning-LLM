from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("quantity").between(138, 208))
    .groupBy("product_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("t.last_last_restock"))
)
