from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.opened_date").between("2023-03-01", "2023-09-01"))
    .groupBy(F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(
        F.count(F.col("ss.units")).alias("units_count"),
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
    )
    .filter(F.col("stddev_revenue") >= 72)
    .orderBy("units_count", F.col("launch_year").desc())
)
