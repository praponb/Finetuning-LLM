from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .select(
        F.col("e.level"),
        F.col("e.hire_date"),
        F.col("e.full_name"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("d.budget"),
    )
)
