from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter((F.col("d.dept_name") == "Legal") | F.col("e.full_name").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.dept_id"),
        F.col("e.full_name"),
        F.col("d.dept_name"),
        F.lower(F.col("d.location")).alias("location_lower"),
    )
    .orderBy(F.col("id").desc(), "location_lower")
    .limit(5)
)
