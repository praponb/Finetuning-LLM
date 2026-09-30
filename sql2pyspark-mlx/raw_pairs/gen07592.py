from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_managers"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.unique_managers") >= 5)
    .select(F.col("d.budget"), F.col("ds.unique_managers"))
    .orderBy(F.col("unique_managers").desc())
)
