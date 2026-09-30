from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("e.manager_id"),
        F.col("e.level"),
        F.col("e.full_name"),
    )
    .orderBy(F.col("manager_id").desc(), "full_name")
)
