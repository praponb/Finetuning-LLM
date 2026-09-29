from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2534000)
    .groupBy(
        F.when(F.col("budget") >= 4342000, "high").when(F.col("budget") >= 973000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min("budget").alias("min_budget"))
    .orderBy("min_budget", "budget_bucket")
    .limit(5)
)
