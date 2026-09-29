from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.revenue")).alias("max_revenue"),
    )
)
