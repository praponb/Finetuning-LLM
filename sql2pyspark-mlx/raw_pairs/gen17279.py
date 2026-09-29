from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.email"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("d.dept_name"),
    )
    .orderBy("budget")
)
