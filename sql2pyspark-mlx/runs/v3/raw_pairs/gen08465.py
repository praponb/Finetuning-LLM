from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.dept_name").isin(["Legal", "HR"]) & F.col("e.email").isNotNull())
    .select(F.col("d.location"), F.col("e.hire_date"), F.col("e.is_remote"))
)
