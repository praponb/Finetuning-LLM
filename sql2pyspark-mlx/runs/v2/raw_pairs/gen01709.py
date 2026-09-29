from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "outer")
    .filter(F.col("d.budget") <= 4004000)
    .select(
        F.col("e.hire_date"),
        F.col("e.job_title"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
    )
)
