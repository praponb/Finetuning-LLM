from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location").isin(["Singapore", "Chiang Mai"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .filter(F.col("e.level").isin(["L1", "L4", "L2"]))
    .select(F.col("e.id"), F.col("e.is_remote"), F.col("e.full_name"))
    .orderBy(F.col("id").desc(), "full_name")
)
