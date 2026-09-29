from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.rating") >= 2.7)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("s.rating"),
    )
    .orderBy(F.col("rating").desc(), "category")
)
