from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.supplier_name").like("%Lion%") & F.col("ss.product_id").isin([2, 6]))
    .select(
        F.col("ss.revenue"),
        F.col("ss.product_id"),
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("s.supplier_name"),
    )
)
