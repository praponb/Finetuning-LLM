from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("ss.product_id").isin([3, 4, 7]))
    .groupBy(F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"),
        F.max(F.col("ss.units")).alias("max_units"),
    )
    .filter(F.col("avg_revenue") > 74)
    .orderBy("opened_month")
)
