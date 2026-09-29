from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2520000)
    .cube(
        "location",
        F.when(F.col("budget") >= 3952000, "high").when(F.col("budget") >= 611000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0))) / F.count("*")), 1).alias("pct_marketing"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)
