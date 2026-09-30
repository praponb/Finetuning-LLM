from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.product_name").like("%wir%"))
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("s.city"),
        F.col("s.rating"),
        F.lower(F.col("s.country")).alias("country_lower"),
    )
    .orderBy(F.col("rating").desc(), "id")
    .limit(50)
)
