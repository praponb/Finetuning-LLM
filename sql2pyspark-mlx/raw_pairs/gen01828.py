from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("manager_id").isin([5, 10, 20]))
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("employees_by_department"), F.col("departments.id") == F.col("employees_by_department.dept_id"), "inner")
    .filter(F.col("employees_by_department.l5_salary") >= 73)
    .select(
        F.col("departments.dept_name"),
        F.col("employees_by_department.stddev_salary"),
        F.col("employees_by_department.l5_salary"),
    )
    .orderBy(F.col("l5_salary").desc())
)
