from pyspark.sql import functions as F

recent_employees = df_employees.filter(F.col("is_remote") | (F.col("salary") < 92000))

employees_stats = (
    recent_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"), "job_title")
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
    )
)

result = (
    employees_stats
    .filter(F.col("l5_salary") >= 74)
    .select("hire_month", "job_title")
    .orderBy("job_title", "hire_month")
)
