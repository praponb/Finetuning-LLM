from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("s.rating"),
    )
    .orderBy("brand", "city")
)
