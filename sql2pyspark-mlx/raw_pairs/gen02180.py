from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.manager_id").isin([13, 23, 33, 36]) & F.col("e.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.dept_id"),
        F.col("e.hire_date"),
        F.col("d.budget"),
        F.col("d.location"),
    )
)
