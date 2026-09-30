from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("manager_id").isNotNull()).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.location") == "New York")
    .select(F.col("d.id"), F.col("d.budget"), F.col("d.location"))
)
