from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.launch_date") >= "2023-04-17")
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.supplier_id"),
        F.col("s.country"),
    )
)
