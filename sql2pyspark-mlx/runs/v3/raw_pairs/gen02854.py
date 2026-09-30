from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "right")
    .select(
        F.col("e.manager_id"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
    .orderBy(F.col("dept_name").desc())
)
