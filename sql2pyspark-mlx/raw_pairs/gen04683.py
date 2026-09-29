from pyspark.sql import functions as F

recent_employees = df_employees.filter(F.col("hire_date") < "2025-05-01")

is_remote_summary = (
    recent_employees
    .groupBy("is_remote")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)

result = is_remote_summary.select("is_remote", "salary_range")
