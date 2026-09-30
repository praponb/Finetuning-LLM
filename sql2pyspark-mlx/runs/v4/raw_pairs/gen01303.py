from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("launch_date").between("2024-11-01", "2025-03-01")).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("ss.product_id"),
    )
)
