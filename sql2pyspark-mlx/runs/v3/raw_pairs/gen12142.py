from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(F.sum(F.col("ss.revenue")).alias("total_revenue"))
    .filter(F.col("total_revenue") > 10000)
    .orderBy(F.col("total_revenue").desc(), "region")
)
