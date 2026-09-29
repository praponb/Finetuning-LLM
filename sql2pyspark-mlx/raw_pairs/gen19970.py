from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.brand"),
        F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"),
    )
    .agg(
        F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"),
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
    )
)
