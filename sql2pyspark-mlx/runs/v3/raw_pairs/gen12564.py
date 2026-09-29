from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter((F.col("d.dept_name") != "Finance") & F.col("e.manager_id").isNotNull())
    .select(
        F.col("e.salary"),
        F.col("e.dept_id"),
        F.col("e.full_name"),
        F.col("d.location"),
    )
)
