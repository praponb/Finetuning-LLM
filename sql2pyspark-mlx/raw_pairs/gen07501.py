from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.manager_id") == 19)
    .select(
        F.col("e.id"),
        F.col("e.is_remote"),
        F.col("e.full_name"),
        F.col("e.hire_date"),
        F.col("d.budget"),
    )
)
