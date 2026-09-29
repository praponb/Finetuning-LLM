from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .groupBy(F.col("s.region"))
    .agg(
        F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"),
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
    )
)
