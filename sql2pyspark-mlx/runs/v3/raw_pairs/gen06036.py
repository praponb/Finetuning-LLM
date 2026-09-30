from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull() & (F.col("budget") >= 2571000))
    .groupBy("location")
    .agg(F.count("*").alias("department_count"))
    .filter(F.col("department_count") > 2)
)
