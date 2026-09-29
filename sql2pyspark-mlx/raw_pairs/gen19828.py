from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location").isin(["Berlin", "Chiang Mai"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .select(F.col("e.id"), F.col("e.full_name"), F.col("e.level"), F.col("e.email"))
    .orderBy("full_name")
)
