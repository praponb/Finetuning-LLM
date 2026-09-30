from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("product_id") == 10).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .filter(F.col("p.launch_date").between("2024-01-01", "2025-06-01"))
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.stock_qty"))
)
