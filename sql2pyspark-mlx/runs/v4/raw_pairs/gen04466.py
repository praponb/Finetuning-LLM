from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"), F.col("s.city"))
    .agg(F.count("*").alias("num_store_sales"))
    .orderBy(F.col("opened_year").desc(), F.col("city").desc())
)
