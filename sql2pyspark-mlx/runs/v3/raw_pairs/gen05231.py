from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_depts"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.location"), F.col("d.dept_name"), F.col("t.unique_depts"))
    .orderBy(F.col("unique_depts").desc())
)
