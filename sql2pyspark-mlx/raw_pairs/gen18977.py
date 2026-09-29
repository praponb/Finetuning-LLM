from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"))
    .orderBy("dept_name", F.col("hr_count").desc())
)
