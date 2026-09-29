from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.opened_date").between("2023-06-03", "2024-09-01") | (F.col("s.opened_date") > "2023-11-01"))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("s.opened_date"),
        F.col("p.category"),
        F.col("p.launch_date"),
    )
)
