from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.is_remote"),
        F.col("e.job_title"),
        F.col("e.full_name"),
        F.col("d.dept_name"),
    )
    .orderBy(F.col("dept_name").desc(), "id")
    .limit(3)
)
