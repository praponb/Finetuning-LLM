from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.filter(F.col("level").isin(["L2", "L4", "L1"])).alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_semi")
    .select(F.col("departments.id"), F.col("departments.location"))
)
