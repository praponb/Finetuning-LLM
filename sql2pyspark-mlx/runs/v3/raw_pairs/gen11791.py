from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.product_name").like("S%"))
    .groupBy(F.col("p.brand"), F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(
        F.avg(F.col("ss.units")).alias("avg_units"),
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
    )
    .orderBy(F.col("avg_units").desc(), "total_revenue")
)
