from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(~F.col("e.level").isin(["L3", "L1"]) & (F.col("d.location") == "Chiang Mai"))
    .select(F.col("d.dept_name"), F.col("d.budget"), F.col("e.dept_id"))
)
