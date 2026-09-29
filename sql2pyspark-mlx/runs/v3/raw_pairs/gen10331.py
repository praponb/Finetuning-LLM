from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("dept_id") == 16)
    .groupBy("dept_id")
    .agg(
        F.max("salary").alias("max_salary"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.min_salary") >= 81000)
    .select(
        F.col("departments.dept_name"),
        F.col("departments.budget"),
        F.col("t.max_salary"),
        F.col("t.min_salary"),
    )
    .orderBy("budget")
)
