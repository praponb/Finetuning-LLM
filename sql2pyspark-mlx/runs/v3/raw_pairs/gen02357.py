from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2023-08-18")
    .groupBy("store_id")
    .agg(F.avg("units").alias("avg_units"))
    .orderBy(F.col("store_id").desc(), F.col("avg_units").desc())
    .limit(1)
)
