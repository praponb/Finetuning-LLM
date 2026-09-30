from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_anti")
    .filter(F.col("departments.location").isNotNull())
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("departments.budget"),
    )
    .orderBy("budget", "id")
    .limit(1)
)
