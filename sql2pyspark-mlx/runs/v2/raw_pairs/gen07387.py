from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.units") != 25)
    .groupBy(F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"))
    .agg((F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"))
    .filter(F.col("units_range") >= 26)
)
