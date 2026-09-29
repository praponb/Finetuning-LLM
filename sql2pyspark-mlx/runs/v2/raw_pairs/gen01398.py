from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.last_restock").between("2024-02-03", "2025-04-01"))
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 3)
    .orderBy(F.col("unique_products").desc())
)
