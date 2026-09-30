from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .groupBy(F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"))
    .agg(
        F.max(F.col("ss.revenue")).alias("max_revenue"),
        F.countDistinct(F.col("ss.units")).alias("unique_products"),
    )
    .orderBy("max_revenue", "launch_month")
    .limit(3)
)
