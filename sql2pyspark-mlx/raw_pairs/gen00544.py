from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.avg_revenue") >= 2200)
    .select(F.col("s.id"), F.col("s.region"), F.col("t.avg_revenue"))
    .orderBy("region", "id")
    .limit(20)
)
