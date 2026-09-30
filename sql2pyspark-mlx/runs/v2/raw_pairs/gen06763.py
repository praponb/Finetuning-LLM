from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
        F.col("s.city"),
    )
    .agg(
        F.max(F.col("ss.revenue")).alias("max_revenue"),
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
    )
    .filter(F.col("max_revenue") >= 1000)
)
