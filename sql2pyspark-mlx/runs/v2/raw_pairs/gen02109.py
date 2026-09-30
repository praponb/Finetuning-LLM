from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.countDistinct("revenue").alias("unique_stores"))
    .filter(F.col("unique_stores") > 5)
    .orderBy(F.col("store_id").desc())
)
