from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .filter(F.col("departments.budget").between(3687000, 3902000) | F.col("employees.email").like("%@mail.co.th"))
    .select(
        F.col("employees.id"),
        F.col("employees.level"),
        F.col("employees.manager_id"),
        F.col("employees.job_title"),
        F.col("departments.dept_name"),
        F.round(F.col("employees.salary"), 2).alias("salary_rounded"),
    )
)
