from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.id"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("e.level")))).alias("level_values"),
        F.count(F.when(F.col("e.level") == "L5", 1)).alias("l5_count"),
    )
    .orderBy(F.col("l5_count").desc(), "id")
    .limit(5)
)
