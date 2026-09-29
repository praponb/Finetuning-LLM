from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("dept_name").isin(["Legal", "Finance", "Engineering"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .filter(F.year(F.col("e.hire_date")) >= 2024)
    .select(F.col("e.id"), F.col("e.hire_date"), F.col("e.email"), F.col("e.dept_id"))
)
