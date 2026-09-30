from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("quantity") != 207)
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
        F.col("ps.unique_stores"),
        F.col("ps.latest_last_restock"),
    )
    .orderBy("brand")
)
