from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("hire_date") > "2024-04-09")
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_managers"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("d.budget"), F.col("t.unique_managers"))
)
