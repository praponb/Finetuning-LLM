from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Legal"]))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

summary = (
    base
    .groupBy(
        F.when(F.col("budget") >= 4036000, "high").when(F.col("budget") >= 1573000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg("budget_filled").alias("avg_budget_filled"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
)

result = (
    summary
    .select("budget_bucket", "avg_budget_filled", "stddev_budget")
    .orderBy(F.col("avg_budget_filled").desc())
)
