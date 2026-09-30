from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .groupBy("store_id")
    .agg(F.max("revenue").alias("max_revenue"))
    .orderBy(F.col("store_id").desc())
)
