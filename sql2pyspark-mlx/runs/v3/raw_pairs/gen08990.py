from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.count(F.col("ss.units")).alias("units_count"),
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
    )
    .orderBy(F.col("total_revenue").desc(), "region")
    .limit(5)
)
