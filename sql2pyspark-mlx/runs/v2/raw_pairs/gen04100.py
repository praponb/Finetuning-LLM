from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.units") == 24)
    .groupBy(
        F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"),
        F.col("p.brand"),
    )
    .agg(F.sum(F.col("ss.revenue")).alias("total_revenue"))
)
