from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("e.full_name").isNull() | F.col("e.job_title").isin(["Engineer", "Manager"]))
    .select(F.col("e.id"), F.col("e.full_name"), F.col("d.dept_name"))
)
