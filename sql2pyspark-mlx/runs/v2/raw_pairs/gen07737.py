from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .select(
        F.col("e.full_name"),
        F.col("e.job_title"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("d.dept_name"),
    )
)
