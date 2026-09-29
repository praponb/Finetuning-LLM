from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.is_remote"),
        F.col("e.dept_id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.concat(F.col("d.dept_name"), F.lit(" - "), F.col("d.location")).alias("dept_name_location"),
    )
    .orderBy("dept_name", "id")
    .limit(20)
)
