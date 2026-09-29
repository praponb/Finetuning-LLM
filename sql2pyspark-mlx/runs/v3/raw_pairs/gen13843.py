from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .filter(F.col("departments.budget") > 2191000)
    .groupBy(F.col("departments.id"), F.col("departments.location"))
    .agg(F.count(F.col("employees.id")).alias("employee_count"))
    .filter(F.col("employee_count") >= 2)
    .orderBy(F.col("id").desc(), F.col("employee_count").desc(), "location")
    .limit(20)
)
