from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.brand").alias("brand"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.revenue")).alias("max_revenue"),
        F.max(F.col("a.units")).alias("max_units"),
    )
    .orderBy(F.col("max_revenue").asc_nulls_last())
)
