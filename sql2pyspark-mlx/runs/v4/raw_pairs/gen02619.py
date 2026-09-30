from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("level") == "L3")
    .groupBy("dept_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "inner")
    .select(F.col("departments.dept_name"), F.col("departments.budget"), F.col("t.cnt"))
)
