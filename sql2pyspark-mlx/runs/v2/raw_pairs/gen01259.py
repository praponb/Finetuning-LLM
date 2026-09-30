from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(~F.col("level").isin(["L1", "L5"])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .select(F.col("d.id"), F.col("d.budget"), F.col("d.location"), F.col("d.dept_name"))
)
