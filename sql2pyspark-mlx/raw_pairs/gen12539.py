from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.units"),
        F.col("p.supplier_id"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
)
