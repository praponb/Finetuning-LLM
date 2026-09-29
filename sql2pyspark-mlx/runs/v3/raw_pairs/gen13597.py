from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("hire_date") <= "2025-10-15").alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.dept_name") == "Legal")
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("d.location"), F.col("d.budget"))
)
