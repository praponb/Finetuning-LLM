from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["Legal", "Finance"]), "core").otherwise("other").alias("dept_name_group"),
        "location",
        F.when(F.col("budget") >= 4137000, "high").when(F.col("budget") >= 911000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.count("budget").alias("budget_count"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)
