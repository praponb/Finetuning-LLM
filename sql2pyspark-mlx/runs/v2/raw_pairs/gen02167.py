from pyspark.sql import functions as F

base = (
    df_departments
    .filter(~F.col("dept_name").isin(["HR", "Legal", "Sales"]) & ~F.col("dept_name").isin(["Finance", "Engineering", "Legal"]))
)

departments_stats = (
    base
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3117000, "high").when(F.col("budget") >= 1630000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
        F.avg("budget").alias("avg_budget"),
    )
)

result = (
    departments_stats
    .filter(F.col("avg_budget") > 3503000)
    .orderBy("location")
)
