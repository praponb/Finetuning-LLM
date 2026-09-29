from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.hire_date"),
        F.col("e.dept_id"),
        F.col("e.level"),
        F.col("d.budget"),
    )
)
