from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "New York"]))
    .groupBy(
        F.when(F.col("budget") >= 4483000, "high").when(F.col("budget") >= 1500000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.count("budget").alias("budget_count"))
    .filter(F.col("budget_count") > 9)
)
