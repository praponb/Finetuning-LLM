from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter((F.col("d.location") == "Berlin") & (F.col("e.hire_date") < "2024-03-06"))
    .select(
        F.col("e.is_remote"),
        F.col("e.level"),
        F.col("e.job_title"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("d.location"),
    )
)
