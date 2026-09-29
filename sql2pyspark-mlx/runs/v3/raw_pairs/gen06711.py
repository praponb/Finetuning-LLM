from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.countDistinct("salary").alias("unique_job_titles"),
    )
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("employees_by_department"), F.col("departments.id") == F.col("employees_by_department.dept_id"), "left")
    .filter(F.col("employees_by_department.unique_job_titles") >= 4)
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.location"),
        F.col("employees_by_department.stddev_salary"),
        F.col("employees_by_department.unique_job_titles"),
    )
)
