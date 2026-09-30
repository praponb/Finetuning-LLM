from pyspark.sql import functions as F

valid_employees = df_employees.filter((F.col("job_title") == "Designer") & (F.col("salary") < 135000))

hire_month_summary = (
    valid_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(F.min("salary").alias("min_salary"))
)

result = (
    hire_month_summary
    .filter(F.col("min_salary") > 119000)
    .select("hire_month", "min_salary")
    .orderBy(F.col("hire_month").desc())
)
