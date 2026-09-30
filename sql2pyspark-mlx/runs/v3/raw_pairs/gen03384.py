from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1720000)
    .groupBy("dept_name")
    .agg(F.count("*").alias("department_count"))
    .filter(F.col("department_count") >= 7)
    .select("dept_name", "department_count")
)
