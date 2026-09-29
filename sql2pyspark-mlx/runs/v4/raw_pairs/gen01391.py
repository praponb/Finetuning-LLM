from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .select(F.col("e.level"), F.col("d.budget"))
    .orderBy("level", F.col("budget").desc())
)
