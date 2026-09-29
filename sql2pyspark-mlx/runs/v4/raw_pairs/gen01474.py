from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("s.opened_date")).isin([2, 7, 11]))
    .groupBy(
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
        F.col("s.region"),
    )
    .agg(
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
        F.count("*").alias("num_store_sales"),
    )
    .orderBy("opened_month")
)
