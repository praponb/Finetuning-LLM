from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(~F.col("email").like("%@gmail.com"))
    .groupBy("dept_id")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.budget"), F.col("d.dept_name"), F.col("t.avg_salary"))
    .orderBy("avg_salary", F.col("budget").desc())
)
