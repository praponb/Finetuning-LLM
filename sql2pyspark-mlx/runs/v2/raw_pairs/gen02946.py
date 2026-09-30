from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .select(F.col("e.is_remote"), F.col("d.budget"), F.col("d.location"))
    .orderBy("location", F.col("budget").desc())
)
