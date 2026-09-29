from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.max("units").alias("max_units"))
    .filter(F.col("max_units") > 32)
    .orderBy(F.col("max_units").desc(), "store_id")
    .limit(20)
)
