from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("employees.hire_date") <= "2023-06-01")
    .select(
        F.col("employees.id"),
        F.col("employees.dept_id"),
        F.col("departments.location"),
        F.col("departments.budget"),
        F.when(F.col("departments.dept_name") == "Engineering", 1).otherwise(0).alias("is_engineering"),
    )
    .orderBy(F.col("id").desc())
)
