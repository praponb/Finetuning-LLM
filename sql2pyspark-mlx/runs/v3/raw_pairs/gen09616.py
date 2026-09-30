from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("s.country"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.size(F.col("p.tags")).alias("tags_count"),
    )
)
