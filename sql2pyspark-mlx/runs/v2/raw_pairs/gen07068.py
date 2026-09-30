from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.filter(F.col("is_remote")).alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_anti")
    .filter(F.col("departments.budget") < 2613000)
    .select(F.col("departments.id"), F.col("departments.location"))
)
