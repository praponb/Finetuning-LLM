from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("ss.revenue") > 1400) & (F.col("ss.units") > 16))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("p.supplier_id"),
        F.col("s.city"),
    )
    .orderBy("supplier_id")
)
