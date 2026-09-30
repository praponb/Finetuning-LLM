from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(~F.col("dept_name").isin(["Finance", "Legal"])).alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_semi")
    .filter(~F.col("employees.email").like("%@startup.io"))
    .select(F.col("employees.id"), F.col("employees.level"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
