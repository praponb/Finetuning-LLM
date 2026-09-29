from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") > 6)
    .select("location", "total_count")
)
