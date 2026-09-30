from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(~F.col("is_remote")).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .filter(F.col("d.dept_name").isNull())
    .select(F.col("d.id"), F.col("d.budget"), F.col("d.location"))
)
