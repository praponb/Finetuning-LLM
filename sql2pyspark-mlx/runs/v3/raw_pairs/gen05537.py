from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .groupBy(F.quarter(F.col("p.launch_date")).alias("launch_quarter"))
    .agg(F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"))
    .filter(F.col("stddev_units") >= 27)
)
