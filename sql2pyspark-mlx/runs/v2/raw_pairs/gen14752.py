from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("budget").isNotNull()).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .select(F.col("e.id"), F.col("e.job_title"))
    .orderBy("job_title", F.col("id").desc())
)
