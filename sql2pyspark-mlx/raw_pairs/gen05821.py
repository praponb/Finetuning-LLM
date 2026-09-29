from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([3, 7]))
    .groupBy("is_remote")
    .agg(F.count("*").alias("employee_count"))
    .orderBy(F.col("employee_count").desc())
)
