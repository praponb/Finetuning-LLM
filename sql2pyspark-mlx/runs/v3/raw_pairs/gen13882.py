from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "Chiang Mai"]) & (F.col("budget") > 2608000))
    .agg(F.count("*").alias("department_count"))
    .orderBy("department_count")
)
