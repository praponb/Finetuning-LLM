from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.dept_id").isin([2, 3, 10]) & F.lower(F.col("e.email")).like("%aro%"))
    .select(
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("e.is_remote"),
        F.col("e.full_name"),
        F.col("e.email"),
    )
)
