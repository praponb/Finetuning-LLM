from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .select(
        F.col("ss.revenue"),
        F.col("s.store_name"),
        F.col("p.cost"),
        F.col("p.launch_date"),
    )
)
