from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.sale_date").between("2024-08-01", "2025-05-01"))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("p.category"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.upper(F.col("s.supplier_name")).alias("supplier_name_upper"),
    )
    .orderBy(F.col("country").desc())
)
