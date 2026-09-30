from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(~F.col("email").like("%@outlook.com") & (F.year("hire_date") >= 2025))
)

hire_quarter_summary = (
    valid_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.max("salary").alias("max_salary"),
        F.min("salary").alias("min_salary"),
    )
)

result = hire_quarter_summary.orderBy("hire_quarter", "max_salary")
