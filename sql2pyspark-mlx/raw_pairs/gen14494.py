from pyspark.sql import functions as F

dim_totals = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("dim"), F.col("e.product_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.units")).alias("min_units"),
        F.min(F.col("e.revenue")).alias("min_revenue"),
    )
)

result = dim_totals.select("min_units")
