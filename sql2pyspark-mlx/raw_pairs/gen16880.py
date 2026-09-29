from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("e.salary"),
        F.col("e.dept_id"),
        F.col("e.is_remote"),
        F.length(F.col("e.job_title")).alias("job_title_length"),
    )
)
