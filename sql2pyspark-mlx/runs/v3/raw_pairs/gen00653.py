from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.length(F.col("s.store_name")) >= 14)
    .groupBy(F.col("s.region"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.avg(F.col("ss.revenue")).alias("avg_revenue"),
    )
    .orderBy(F.col("store_sale_count").desc(), F.col("region").desc())
    .limit(10)
)
