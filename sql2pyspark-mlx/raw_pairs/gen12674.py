from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4393000, "high").when(F.col("budget") >= 2055000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .filter(F.col("total_count") > 6)
    .select("budget_bucket", "total_count", "stddev_budget")
    .orderBy("stddev_budget", "budget_bucket")
    .limit(3)
)
