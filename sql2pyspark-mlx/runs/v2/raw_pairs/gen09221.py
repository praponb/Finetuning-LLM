from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("p.supplier_id"),
        F.col("s.rating"),
        F.col("s.city"),
        F.array_join(F.col("p.tags"), "|").alias("tags_text"),
    )
    .orderBy(F.col("last_restock").desc())
)
