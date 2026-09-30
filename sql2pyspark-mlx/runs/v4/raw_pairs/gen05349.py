from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .filter(F.col("departments.budget") <= 2497000)
    .select(
        F.col("employees.salary"),
        F.col("employees.manager_id"),
        F.col("employees.email"),
        F.col("departments.budget"),
        F.col("departments.location"),
    )
    .orderBy("manager_id")
)
