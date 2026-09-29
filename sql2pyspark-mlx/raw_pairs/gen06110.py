from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.price") >= 940)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.col("s.city"),
        F.col("s.country"),
        F.lower(F.col("s.supplier_name")).alias("supplier_name_lower"),
    )
    .orderBy(F.col("id").desc())
)
