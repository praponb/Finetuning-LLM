from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5]) & (F.col("units") >= 22))
    .groupBy("store_id")
    .agg(F.min("units").alias("min_units"))
    .orderBy(F.col("store_id").desc())
)
