from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .groupBy(
        F.when(F.col("d.budget") >= 4263000, "high").when(F.col("d.budget") >= 1299000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("d.dept_name"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("e.level")))).alias("level_values"),
    )
)
