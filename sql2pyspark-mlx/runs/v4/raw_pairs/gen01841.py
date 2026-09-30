from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.dept_id").isin([24, 26, 31, 39]))
    .select(
        F.col("e.job_title"),
        F.col("e.is_remote"),
        F.col("d.dept_name"),
        F.substring(F.col("d.location"), 1, 4).alias("location_prefix"),
    )
)
