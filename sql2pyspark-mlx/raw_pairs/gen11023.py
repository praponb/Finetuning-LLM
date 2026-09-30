from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.filter(F.col("job_title").isin(["Manager", "Analyst"])).alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_anti")
    .filter(F.col("departments.dept_name") == "Legal")
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.col("departments.location"),
    )
    .orderBy(F.col("budget").desc(), "location")
)
