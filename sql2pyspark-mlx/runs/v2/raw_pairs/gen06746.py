from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("p.product_name"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
)
