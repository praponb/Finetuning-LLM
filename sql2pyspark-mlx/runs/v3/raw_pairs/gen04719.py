from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("dept_name").isNull()).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .filter(F.col("e.level").isin(["L5", "L3", "L4"]))
    .select(F.col("e.id"), F.col("e.job_title"), F.col("e.full_name"))
    .orderBy(F.col("id").desc())
)
