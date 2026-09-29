from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("i.last_restock").between("2024-10-01", "2025-06-10"))
    .groupBy(F.col("p.category"))
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
)
