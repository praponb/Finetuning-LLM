from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(F.min("salary").alias("min_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.budget"), F.col("d.dept_name"), F.col("t.min_salary"))
    .orderBy("dept_name", F.col("min_salary").desc())
)
