from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("launch_date").between("2023-03-12", "2024-08-01")).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
    )
)
