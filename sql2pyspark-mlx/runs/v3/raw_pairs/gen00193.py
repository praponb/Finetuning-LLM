from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .select(
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("e.job_title"),
        F.length(F.col("d.location")).alias("location_length"),
    )
    .orderBy("dept_name")
)
