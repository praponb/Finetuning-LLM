from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count("*").alias("emp_count"))
    .filter(F.col("emp_count") > 5)
)
