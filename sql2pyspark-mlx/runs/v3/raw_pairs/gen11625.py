from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.count("*").alias("total_count"),
        F.max(F.col("ss.revenue")).alias("max_revenue"),
    )
    .filter(F.col("total_count") > 5)
)
