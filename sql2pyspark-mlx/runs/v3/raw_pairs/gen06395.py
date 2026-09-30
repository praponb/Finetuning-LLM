from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("location").isin(["Berlin", "Tokyo", "Singapore"]))

departments_stats = (
    valid_departments
    .groupBy("location")
    .agg(
        F.max("budget").alias("max_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)

result = departments_stats.select("location", "max_budget", "budget_range")
