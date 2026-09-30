from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(F.col("dept_name").isNotNull()).alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_semi")
    .select(
        F.col("employees.id"),
        F.col("employees.manager_id"),
        F.col("employees.salary"),
        F.col("employees.level"),
    )
    .orderBy("id", "manager_id")
)
