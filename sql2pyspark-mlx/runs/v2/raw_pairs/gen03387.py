from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.filter(F.col("job_title").isin(["Analyst", "Manager"])).alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_anti")
    .filter(F.col("departments.location") == "London")
    .select(F.col("departments.id"), F.col("departments.dept_name"))
)
