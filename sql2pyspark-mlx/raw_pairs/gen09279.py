from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .filter((F.col("employees.job_title") == "Engineer") & (F.col("departments.location") != "Berlin"))
    .select(
        F.col("departments.dept_name"),
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("employees.salary"),
        F.col("employees.level"),
    )
    .orderBy("level")
)
