from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("ss.units").between(30, 34))
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(F.min(F.col("ss.units")).alias("min_units"))
    .orderBy(F.col("min_units").desc())
)
