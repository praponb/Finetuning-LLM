from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating") >= 4.1)
    .select(
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("p.brand"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
)
