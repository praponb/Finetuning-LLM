from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .filter(F.col("employees.manager_id").isin([20, 33, 35, 40]))
    .select(
        F.col("employees.id"),
        F.col("employees.manager_id"),
        F.col("employees.job_title"),
        F.col("employees.email"),
        F.col("departments.budget"),
    )
    .orderBy("manager_id")
)
