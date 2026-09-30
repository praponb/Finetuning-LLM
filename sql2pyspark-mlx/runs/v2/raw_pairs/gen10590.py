from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3184000, "high").when(F.col("budget") >= 1757000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.max("budget").alias("max_budget"),
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget")).otherwise(0)).alias("tokyo_budget"),
    )
    .filter(F.col("max_budget") < 2417000)
    .select("budget_bucket", "dept_name", "max_budget", "tokyo_budget")
)
