from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("s.city") == "London") & (F.year(F.col("ss.sale_date")) == 2025))
    .select(F.col("ss.store_id"), F.col("p.launch_date"), F.col("s.country"))
)
