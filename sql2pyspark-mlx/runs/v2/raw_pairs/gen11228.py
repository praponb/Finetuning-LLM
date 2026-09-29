from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.is_remote"),
        F.col("e.job_title"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
