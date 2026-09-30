from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Bangkok", "Tokyo"]))
    .groupBy(
        F.when(F.col("budget") >= 3350000, "high").when(F.col("budget") >= 1665000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("max_budget") >= 3237000)
    .select("budget_bucket", "location", "total_budget", "max_budget")
)
