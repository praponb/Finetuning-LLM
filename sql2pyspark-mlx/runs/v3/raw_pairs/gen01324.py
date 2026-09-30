from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.location").isin(["Chiang Mai", "New York"]))
    .groupBy(
        F.when(F.col("d.budget") >= 4403000, "high").when(F.col("d.budget") >= 906000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"),
        F.min(F.col("e.salary")).alias("min_salary"),
    )
)
