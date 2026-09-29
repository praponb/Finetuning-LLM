from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("ss.sale_date").between("2023-12-01", "2025-08-01"))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
    )
)
