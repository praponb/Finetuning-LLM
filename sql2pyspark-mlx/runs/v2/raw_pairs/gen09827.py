from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.full_name").isNotNull() | (F.col("e.salary") < 136000))
    .select(
        F.col("e.level"),
        F.col("e.is_remote"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
    .orderBy("location")
)
