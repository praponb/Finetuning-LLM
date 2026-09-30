from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter((F.col("e.hire_date") >= "2024-12-01") & (F.col("d.dept_name") == "Finance"))
    .select(
        F.col("e.id"),
        F.col("e.is_remote"),
        F.col("e.salary"),
        F.col("e.email"),
        F.col("d.dept_name"),
        F.col("d.location"),
    )
    .orderBy(F.col("dept_name").desc())
)
