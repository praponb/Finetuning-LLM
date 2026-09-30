from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("hire_date") > "2025-10-09") & ~F.col("job_title").isin(["Designer", "Analyst", "Manager"]))
)

hire_month_summary = (
    base
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(F.sum("salary").alias("total_salary"))
)

result = hire_month_summary
