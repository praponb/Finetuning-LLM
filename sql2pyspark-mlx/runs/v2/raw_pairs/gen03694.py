from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.budget").between(1788000, 3158000))
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("d.budget"))
)
