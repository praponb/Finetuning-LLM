from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.length(F.col("e.email")) >= 5)
    .groupBy(
        F.when(F.col("d.budget") >= 3673000, "high").when(F.col("d.budget") >= 1588000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("d.location"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("e.level")))).alias("level_values"),
        F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"),
    )
    .filter(F.col("avg_salary") > 22)
    .orderBy("avg_salary", "budget_bucket", "location")
    .limit(5)
)
