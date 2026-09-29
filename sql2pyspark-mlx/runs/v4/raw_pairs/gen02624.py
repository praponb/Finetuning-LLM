from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("dept_name").isin(["Legal", "Marketing"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .filter(F.month(F.col("e.hire_date")).isin([2, 5, 10]))
    .select(F.col("e.id"), F.col("e.hire_date"), F.col("e.level"))
)
