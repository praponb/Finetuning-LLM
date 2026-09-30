from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.dept_id").isin([9, 10, 12, 24]))
    .groupBy(F.col("d.location"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("e.level")))).alias("level_values"),
        F.min(F.col("e.hire_date")).alias("first_hire_date"),
    )
)
