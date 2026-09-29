from pyspark.sql import functions as F

src = (
    df_departments
    .filter((F.col("budget") <= 3553000) & ((F.col("location") == "Bangkok") | F.col("budget").between(1142000, 1724000)))
)

budget_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("budget") >= 3695000, "high").when(F.col("budget") >= 1034000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.sum(F.when(F.col("location") == "Berlin", F.col("budget")).otherwise(0)).alias("berlin_budget"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
)

result = (
    budget_bucket_summary
    .filter(F.col("stddev_budget") >= 73)
    .select("budget_bucket", "dept_name", "berlin_budget", "stddev_budget")
    .orderBy("dept_name", "berlin_budget", "budget_bucket")
    .limit(20)
)
