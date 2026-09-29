from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter((F.col("d.dept_name") == "Engineering") & (F.col("d.dept_name") == "Finance"))
    .select(
        F.col("e.id"),
        F.col("e.is_remote"),
        F.col("e.level"),
        F.col("d.budget"),
        F.lower(F.col("e.level")).alias("level_lower"),
    )
    .orderBy(F.col("level_lower").desc(), "budget")
)
