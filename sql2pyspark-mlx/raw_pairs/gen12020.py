from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("s.units")).alias("total_units"),
        F.min(F.col("s.revenue")).alias("min_revenue"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("rows_total")
)
