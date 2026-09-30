from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        F.col("d.dept_name"),
        F.col("e.job_title"),
        F.col("e.email"),
        F.col("e.is_remote"),
    )
)
