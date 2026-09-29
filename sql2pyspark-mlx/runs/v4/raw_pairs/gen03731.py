from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.min("hire_date").alias("first_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .select(F.col("d.budget"), F.col("dt.total_salary"), F.col("dt.first_hire_date"))
)
