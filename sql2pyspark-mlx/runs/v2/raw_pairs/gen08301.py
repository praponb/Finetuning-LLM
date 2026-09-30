from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.rating").between(2.8, 3.7))
    .select(
        F.col("ss.revenue"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("s.supplier_name"),
    )
    .orderBy(F.col("supplier_name").desc())
)
