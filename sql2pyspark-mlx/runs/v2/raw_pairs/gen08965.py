from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.avg_revenue") >= 49)
    .select(
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("ss.min_sale_date"),
        F.col("ss.avg_revenue"),
    )
    .orderBy(F.col("avg_revenue").desc(), F.col("sqft").desc())
)
