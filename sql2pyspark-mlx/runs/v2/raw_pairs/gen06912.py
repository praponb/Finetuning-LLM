from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3602000, "high").when(F.col("budget") >= 1546000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
    .filter(F.col("total_budget") > 22536000)
    .select("budget_bucket", "total_budget")
)
