from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.length(F.col("s.supplier_name")) > 11) & F.col("s.country").isin(["US", "SG", "UK"]))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("s.country"),
    )
)
