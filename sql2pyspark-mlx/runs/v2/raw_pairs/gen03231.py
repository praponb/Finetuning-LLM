from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("level") == "L4")
    .groupBy("dept_id")
    .agg(F.sum("salary").alias("total_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.id"), F.col("d.budget"), F.col("t.total_salary"))
)
