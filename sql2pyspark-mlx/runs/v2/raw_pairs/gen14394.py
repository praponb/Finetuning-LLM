from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("p.brand") == "Initech") & F.col("p.supplier_id").isin([4, 30]))
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.category"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        (F.col("p.price") - F.col("p.cost")).alias("margin"),
    )
    .orderBy(F.col("id").desc())
)
