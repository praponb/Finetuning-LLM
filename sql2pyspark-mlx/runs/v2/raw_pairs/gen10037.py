from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("hire_date") < "2023-07-28").alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .filter(~F.col("d.location").isin(["London", "Tokyo"]))
    .select(F.col("d.id"), F.col("d.location"), F.col("d.dept_name"))
    .orderBy("dept_name")
)
