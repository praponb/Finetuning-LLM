from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(~F.col("dept_name").isin(["HR", "Engineering"]) & (F.col("budget") > 2342000))
)

summary = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 3750000, "high").when(F.col("budget") >= 1383000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.sum(F.when(F.col("location") == "New York", 1).otherwise(0)).alias("new_york_count"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
)

result = (
    summary
    .select("budget_bucket", "dept_name", "stddev_budget")
    .orderBy(F.col("stddev_budget").desc(), "budget_bucket")
)
