from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter(F.col("e.full_name").like("Mi%"))
    .groupBy(F.col("d.location"))
    .agg(F.sort_array(F.collect_set(F.col("e.level"))).alias("level_list"))
    .orderBy(F.col("location").desc())
)
