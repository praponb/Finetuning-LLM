from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "right")
    .filter(F.col("departments.dept_name").isin(["Legal", "Engineering", "HR"]))
    .groupBy(F.col("departments.location"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("employees.job_title")))).alias("job_title_values"),
        (F.max(F.col("employees.salary")) - F.min(F.col("employees.salary"))).alias("salary_range"),
    )
    .orderBy(F.col("location").desc(), "job_title_values")
    .limit(10)
)
