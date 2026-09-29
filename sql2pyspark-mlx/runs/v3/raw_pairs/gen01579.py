from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.year(F.col("s.opened_date")) >= 2023)
    .groupBy(F.col("p.category"))
    .agg(
        F.count("*").alias("num_store_sales"),
        F.max(F.col("ss.revenue")).alias("max_revenue"),
    )
    .orderBy(F.col("category").desc())
)
