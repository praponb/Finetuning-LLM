from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_depts"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("D"), F.col("d.id") == F.col("D.dept_id"), "left")
    .filter(F.col("D.unique_depts") > 9)
    .select(F.col("d.id"), F.col("d.location"), F.col("D.unique_depts"))
)
