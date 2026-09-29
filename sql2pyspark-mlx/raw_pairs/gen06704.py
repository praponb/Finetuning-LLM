from pyspark.sql import functions as F

recent_employees = df_employees.filter(~F.col("is_remote"))

employees_stats = (
    recent_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.min("salary").alias("min_salary"),
        F.max("hire_date").alias("last_hire_date"),
    )
)

result = employees_stats.select("hire_month", "min_salary")
