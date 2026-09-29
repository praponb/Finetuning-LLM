from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.hire_date") < "2024-01-27")
    .select(
        F.col("e.id"),
        F.col("e.dept_id"),
        F.col("e.full_name"),
        F.col("e.manager_id"),
        F.col("d.dept_name"),
    )
)
