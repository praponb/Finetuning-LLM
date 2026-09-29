from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location") == "Tokyo").alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .filter(F.col("e.email").isNotNull())
    .select(F.col("e.id"), F.col("e.dept_id"), F.col("e.salary"), F.col("e.job_title"))
    .orderBy(F.col("id").desc())
)
