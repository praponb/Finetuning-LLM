from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter((F.col("p.price") <= 990) | F.col("ss.units").between(21, 32))
    .select(
        F.col("ss.store_id"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("s.country"),
    )
)
