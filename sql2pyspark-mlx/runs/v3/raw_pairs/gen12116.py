from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("employees.manager_id").isNull())
    .select(
        F.col("departments.budget"),
        F.col("employees.job_title"),
        F.col("employees.hire_date"),
        F.col("employees.is_remote"),
        F.concat_ws("-", F.col("employees.full_name"), F.col("employees.level")).alias("full_name_level_key"),
    )
    .orderBy("full_name_level_key")
)
