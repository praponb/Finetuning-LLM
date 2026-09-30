from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter((F.length(F.col("e.full_name")) > 13) & (F.length(F.col("e.full_name")) >= 12))
    .select(
        F.col("e.id"),
        F.col("e.is_remote"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
    )
)
