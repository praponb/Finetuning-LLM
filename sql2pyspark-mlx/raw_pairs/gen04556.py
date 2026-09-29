from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.revenue")).alias("min_revenue"),
        F.min(F.col("s.units")).alias("min_units"),
    )
)
