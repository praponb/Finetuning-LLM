from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("ss.revenue") <= 1200)
    .select(
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("p.launch_date"),
    )
)
