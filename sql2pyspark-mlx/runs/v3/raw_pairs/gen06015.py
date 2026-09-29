from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.budget"), F.col("t.latest_hire_date"))
    .orderBy("budget", "id")
    .limit(20)
)
