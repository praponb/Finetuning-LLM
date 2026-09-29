from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(
        F.when(F.col("d.budget") >= 3404000, "high").when(F.col("d.budget") >= 1558000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count(F.when(F.col("e.level") == "L5", 1)).alias("l5_count"))
    .orderBy(F.col("l5_count").desc())
)
