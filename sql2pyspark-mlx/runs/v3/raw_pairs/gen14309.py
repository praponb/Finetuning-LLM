from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"))
    .agg(
        F.count(F.col("ss.units")).alias("units_count"),
        F.max(F.col("ss.units")).alias("max_units"),
    )
    .orderBy(F.col("launch_month").desc())
)
