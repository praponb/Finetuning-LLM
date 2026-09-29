from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.filter(F.col("email").like("%@yahoo.com")).alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_semi")
    .filter(F.col("departments.location").isin(["London", "Singapore", "Berlin"]))
    .select(
        F.col("departments.id"),
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("departments.dept_name"),
    )
    .orderBy("dept_name", "id")
    .limit(10)
)
