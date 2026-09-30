from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.dept_id").isin([10, 14, 20, 38]) & F.col("e.full_name").like("%Chen%"))
    .select(
        F.col("d.budget"),
        F.col("e.hire_date"),
        F.col("e.level"),
        F.col("e.salary"),
    )
)
