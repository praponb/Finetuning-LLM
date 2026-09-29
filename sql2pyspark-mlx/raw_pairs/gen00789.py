from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("e.email").isNotNull() & (F.col("d.dept_name") != "Finance"))
    .select(
        F.col("e.id"),
        F.col("e.hire_date"),
        F.col("e.job_title"),
        F.col("d.budget"),
    )
)
