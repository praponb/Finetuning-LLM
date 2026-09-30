from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.location").isin(["Berlin", "Chiang Mai"]))
    .select(F.col("d.id"), F.col("d.budget"))
)
