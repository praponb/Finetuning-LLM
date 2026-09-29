from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.min("units").alias("min_units"))
    .filter(F.col("min_units") >= 29)
    .select("store_id", "min_units")
)
