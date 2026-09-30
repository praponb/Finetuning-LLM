from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.product_id"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("s.country"),
    )
    .orderBy(F.col("product_id").desc())
)
