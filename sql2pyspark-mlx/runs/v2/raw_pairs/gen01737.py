from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("e.job_title"),
        F.col("e.is_remote"),
        F.col("e.dept_id"),
    )
    .orderBy(F.col("job_title").desc())
)
