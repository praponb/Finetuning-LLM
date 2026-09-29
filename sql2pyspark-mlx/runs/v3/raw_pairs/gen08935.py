from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.col("ss.units").between(23, 31) & (F.col("s.country") != "DE"))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("p.price"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
    )
)
