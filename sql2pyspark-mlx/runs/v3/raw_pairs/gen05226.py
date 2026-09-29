from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.brand"),
        F.col("s.store_name"),
    )
)
