from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.year("hire_date") >= 2024).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.location").isin(["New York", "Tokyo"]))
    .select(F.col("d.id"), F.col("d.location"), F.col("d.dept_name"))
)
