from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("e.dept_id"),
        F.col("e.manager_id"),
        F.col("e.salary"),
    )
    .orderBy(F.col("manager_id").desc())
)
