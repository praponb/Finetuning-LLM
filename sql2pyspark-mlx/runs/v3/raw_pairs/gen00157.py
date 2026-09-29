from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"), F.col("s.city"))
    .agg(F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"))
    .filter(F.col("avg_revenue") >= 27)
    .orderBy(F.col("city").desc(), "opened_year")
    .limit(1)
)
