from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 4)
    .orderBy(F.col("row_count").desc())
)
