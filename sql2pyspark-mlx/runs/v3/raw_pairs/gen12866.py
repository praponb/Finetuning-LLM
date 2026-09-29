from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.sum(F.col("ss.units")).alias("total_units"),
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
    )
)
