from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.job_title") == "Engineer")
    .select(
        F.col("d.dept_name"),
        F.col("e.email"),
        F.col("e.job_title"),
        F.col("e.dept_id"),
        F.date_sub(F.col("e.hire_date"), 60).alias("hire_date_minus_60d"),
    )
)
