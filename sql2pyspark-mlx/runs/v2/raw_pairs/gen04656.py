from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("budget") > 3880000)

departments_stats = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 3219000, "high").when(F.col("budget") >= 1409000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg("budget").alias("avg_budget"),
    )
)

result = departments_stats.select("budget_bucket", "cnt", "avg_budget")
