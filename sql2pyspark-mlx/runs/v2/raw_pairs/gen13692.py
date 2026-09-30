from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.length(F.col("s.supplier_name")) > 5) & (F.col("s.city") == "New York"))
    .select(
        F.col("s.country"),
        F.col("s.city"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
        F.length(F.col("s.city")).alias("city_length"),
    )
    .orderBy(F.col("city_length").desc())
)
