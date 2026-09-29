from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("last_restock").between("2023-03-01", "2024-04-01"))
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.max_quantity") >= 70)
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("ps.max_quantity"))
    .orderBy(F.col("max_quantity").desc())
)
