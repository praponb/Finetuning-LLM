from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2143000)
    .groupBy(
        F.when(F.col("budget") >= 3340000, "high").when(F.col("budget") >= 700000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0))) / F.count("*")), 1).alias("pct_marketing"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
    )
    .orderBy("dept_name")
)
