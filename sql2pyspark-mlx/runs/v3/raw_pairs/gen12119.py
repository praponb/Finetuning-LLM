from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("e.salary") > 119000)
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("e.email"),
        F.round(F.col("d.budget"), 0).alias("budget_rounded"),
    )
)
