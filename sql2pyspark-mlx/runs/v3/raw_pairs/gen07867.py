from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.store_name").isNotNull())
    .groupBy(F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.min(F.col("ss.units")).alias("min_units"),
        F.countDistinct(F.col("ss.revenue")).alias("unique_products"),
    )
    .orderBy("opened_month")
)
