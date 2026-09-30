from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("employees_by_department"), F.col("departments.id") == F.col("employees_by_department.dept_id"), "inner")
    .filter(F.col("employees_by_department.stddev_salary") >= 50)
    .select(
        F.col("departments.location"),
        F.col("employees_by_department.stddev_salary"),
        F.col("employees_by_department.job_title_values"),
    )
    .orderBy("location", "job_title_values")
)
