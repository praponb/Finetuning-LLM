from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.email").isNotNull())
    .select(
        F.col("e.hire_date"),
        F.col("e.full_name"),
        F.col("e.is_remote"),
        F.col("d.dept_name"),
        F.upper(F.col("d.location")).alias("location_upper"),
    )
    .orderBy("full_name", F.col("location_upper").desc())
)
