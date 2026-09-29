from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location").isin(["Tokyo", "Singapore", "Bangkok"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .filter(F.col("e.hire_date") >= "2024-06-01")
    .select(F.col("e.id"), F.col("e.level"))
)
