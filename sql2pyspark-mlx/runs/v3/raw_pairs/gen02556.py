from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.col("level") == "L5")

hire_quarter_summary = (
    valid_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"), "is_remote")
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    hire_quarter_summary
    .filter(F.col("min_salary") < 66000)
    .select("hire_quarter", "is_remote", "min_hire_date", "min_salary")
    .orderBy(F.col("min_salary").desc(), "hire_quarter", "is_remote")
    .limit(50)
)
