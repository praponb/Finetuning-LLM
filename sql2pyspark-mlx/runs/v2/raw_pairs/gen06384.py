from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("e.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.level"),
        F.col("e.job_title"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
)
