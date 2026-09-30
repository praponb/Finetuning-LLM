from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.units") == 29)
    .groupBy(F.col("s.city"))
    .agg(F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"))
)
