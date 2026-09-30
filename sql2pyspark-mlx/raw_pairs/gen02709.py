from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("d.budget") >= 3600000)
    .select(
        F.col("e.id"),
        F.col("e.is_remote"),
        F.col("e.job_title"),
        F.col("d.budget"),
    )
)
