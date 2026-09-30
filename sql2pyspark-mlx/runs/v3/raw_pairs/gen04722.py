from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.sum(F.col("ss.units")).alias("total_units"))
    .filter(F.col("total_units") >= 170)
    .orderBy(F.col("total_units").desc(), "city")
    .limit(50)
)
