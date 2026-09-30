from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("s.country"),
    )
    .orderBy(F.col("id").desc())
)
