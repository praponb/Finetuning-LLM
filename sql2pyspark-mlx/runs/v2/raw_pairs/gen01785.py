from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.max("salary").alias("max_salary"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("DepartmentTotals"), F.col("departments.id") == F.col("DepartmentTotals.dept_id"), "left")
    .filter(F.col("DepartmentTotals.max_salary") >= 147000)
    .select(
        F.col("departments.id"),
        F.col("departments.location"),
        F.col("departments.dept_name"),
        F.col("DepartmentTotals.max_salary"),
        F.col("DepartmentTotals.avg_salary"),
    )
)
