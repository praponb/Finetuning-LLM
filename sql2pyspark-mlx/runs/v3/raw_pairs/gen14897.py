from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter((F.col("d.location") == "Singapore") & F.col("e.manager_id").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.level"),
        F.col("e.salary"),
        F.col("d.budget"),
        F.col("d.dept_name"),
    )
)
