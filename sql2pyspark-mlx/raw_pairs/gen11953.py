from pyspark.sql import functions as F

result = (
    df_employees
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.location") == "Bangkok")
    .select(
        "s.id",
        "s.full_name",
        "s.dept_id",
        "s.job_title",
        "s.salary",
        "s.hire_date",
        F.col("d.budget"),
        F.col("d.dept_name"),
    )
)
