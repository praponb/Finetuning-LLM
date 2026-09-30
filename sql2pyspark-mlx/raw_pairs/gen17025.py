from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3780000, "high").when(F.col("budget") >= 1648000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
    .filter(F.col("total_budget") < 23411000)
    .select("budget_bucket", "total_budget")
    .orderBy(F.col("total_budget").desc(), "budget_bucket")
    .limit(50)
)
