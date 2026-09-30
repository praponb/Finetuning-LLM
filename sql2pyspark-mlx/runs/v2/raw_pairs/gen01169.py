from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("level").isin(["L4", "L5"])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.location").isin(["New York", "Chiang Mai"]))
    .select(F.col("d.id"), F.col("d.dept_name"))
)
