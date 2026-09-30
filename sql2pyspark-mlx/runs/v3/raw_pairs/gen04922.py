from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Finance"]) & (F.col("location") == "London"))
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4459000, "high").when(F.col("budget") >= 615000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
    )
    .orderBy("dept_name", "budget_bucket")
    .limit(5)
)
