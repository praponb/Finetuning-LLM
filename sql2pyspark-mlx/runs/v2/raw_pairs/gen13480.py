from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.dept_name").isin(["Legal", "Sales", "Engineering"]))
    .select(
        F.col("e.dept_id"),
        F.col("e.is_remote"),
        F.col("e.full_name"),
        F.col("d.dept_name"),
    )
    .orderBy(F.col("dept_id").desc())
)
