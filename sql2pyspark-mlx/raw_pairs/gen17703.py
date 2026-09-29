from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"),
        F.count(F.col("ss.units")).alias("units_count"),
    )
    .orderBy("units_count", F.col("avg_revenue").desc(), "city")
    .limit(50)
)
