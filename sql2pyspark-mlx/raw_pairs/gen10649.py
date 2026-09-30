from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1702000, 3829000))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4302000, "high").when(F.col("budget") >= 1047000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0))) / F.count("*")), 1).alias("pct_hr"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("pct_hr") > 71)
    .orderBy(F.col("max_budget").desc(), "location", "budget_bucket")
    .limit(5)
)
