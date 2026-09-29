from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.full_name").isNotNull())
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("e.dept_id"),
        F.trim(F.col("e.full_name")).alias("clean_full_name"),
    )
)
