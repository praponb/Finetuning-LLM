from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "outer")
    .filter(F.lower(F.col("e.full_name")).like("%rac%"))
    .select(F.col("e.manager_id"), F.col("d.budget"))
    .orderBy("budget")
)
