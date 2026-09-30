from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") != "Director") | (F.col("hire_date") >= "2024-12-31"))
    .groupBy("job_title", "is_remote")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )
    .orderBy("job_title")
)
