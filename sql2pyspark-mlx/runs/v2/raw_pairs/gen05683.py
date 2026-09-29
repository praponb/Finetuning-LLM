from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("budget").between(1118000, 1464000) & F.col("budget").between(2788000, 3461000))
)

agg = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3990000, "high").when(F.col("budget") >= 1114000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
    )
)

result = (
    agg
    .filter(F.col("new_york_budget") >= 34)
    .select("budget_bucket", "new_york_budget")
    .orderBy("new_york_budget", "budget_bucket")
    .limit(10)
)
