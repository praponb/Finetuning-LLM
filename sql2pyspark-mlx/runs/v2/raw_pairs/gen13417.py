from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter((F.col("hire_date") >= "2025-07-01") & (F.col("hire_date") <= "2024-09-09"))
)

employees_stats = (
    recent_employees
    .groupBy("level", F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = (
    employees_stats
    .filter(F.col("avg_salary") < 109000)
    .orderBy("level")
)
