from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"))
)
