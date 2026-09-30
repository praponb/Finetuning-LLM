from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.lower(F.trim(F.col("s.supplier_name"))) == "global supply") & (F.col("p.price") < 780))
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("s.supplier_name"),
    )
)
