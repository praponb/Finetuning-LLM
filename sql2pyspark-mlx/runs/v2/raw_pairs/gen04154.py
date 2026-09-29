from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("budget") > 2054000).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .select(F.col("e.id"), F.col("e.job_title"), F.col("e.full_name"), F.col("e.level"))
)
