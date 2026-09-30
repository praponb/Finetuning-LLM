from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 8)
    .select("region", "city", "row_count")
    .orderBy(F.col("city").desc())
)
