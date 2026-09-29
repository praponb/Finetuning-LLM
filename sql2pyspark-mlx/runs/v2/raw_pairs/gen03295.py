from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "right")
    .filter(F.col("e.email").like("%@gmail.com") & F.col("d.budget").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.is_remote"),
        F.col("d.budget"),
        F.col("d.dept_name"),
    )
)
