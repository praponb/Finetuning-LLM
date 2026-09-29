from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3797000, "high").when(F.col("budget") >= 804000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min("budget").alias("min_budget"))
    .filter(F.col("min_budget") >= 2440000)
    .orderBy("min_budget", "dept_name", "budget_bucket")
    .limit(10)
)
