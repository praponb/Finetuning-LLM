from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.last_restock").between("2023-03-16", "2024-05-06"))
    .select(
        F.col("i.product_id"),
        F.col("p.cost"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.date_format(F.col("i.last_restock"), "yyyy-MM-dd").alias("last_restock_label"),
    )
    .orderBy(F.col("supplier_id").desc())
)
