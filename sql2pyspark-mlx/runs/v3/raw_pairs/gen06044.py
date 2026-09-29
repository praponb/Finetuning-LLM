from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.budget"), F.col("t.director_count"))
    .orderBy("id")
)
