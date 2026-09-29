from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.max(F.col("ss.revenue")).alias("max_revenue"),
        F.sum(F.col("ss.units")).alias("total_units"),
    )
    .orderBy(F.col("total_units").desc())
)
