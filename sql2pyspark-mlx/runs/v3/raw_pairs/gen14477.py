from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.min(F.col("ss.units")).alias("min_units"),
        F.avg(F.col("ss.revenue")).alias("avg_revenue"),
    )
    .orderBy(F.col("id").desc())
)
