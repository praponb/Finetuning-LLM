from pyspark.sql import functions as F

src = (
    df_departments
    .filter(F.col("location").isin(["New York", "Tokyo", "London"]) | F.col("location").isin(["Bangkok", "Singapore", "New York"]))
)

summary = (
    src
    .groupBy(
        F.when(F.col("budget") >= 3617000, "high").when(F.col("budget") >= 1086000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.max("budget").alias("max_budget"),
        F.min("budget").alias("min_budget"),
    )
)

result = (
    summary
    .filter(F.col("max_budget") >= 1305000)
    .orderBy("min_budget", "budget_bucket")
    .limit(3)
)
