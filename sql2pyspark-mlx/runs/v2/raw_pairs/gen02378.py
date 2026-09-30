from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("s.rating") > 2.4) & (F.col("s.city") == "Bangkok"))
    .select(
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("s.supplier_name"),
        F.col("s.city"),
    )
)
