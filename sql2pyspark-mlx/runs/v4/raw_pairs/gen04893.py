from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(~F.col("email").like("%@outlook.com")).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.location") == "Berlin")
    .select(F.col("d.id"), F.col("d.budget"), F.col("d.location"), F.col("d.dept_name"))
)
