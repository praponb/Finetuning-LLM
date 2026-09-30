from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("ss.revenue") <= 2900) & (F.col("s.country") != "JP"))
    .select(
        F.col("ss.product_id"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("s.country"),
        F.col("s.rating"),
    )
)
