from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1697000)
    .groupBy(
        F.when(F.col("budget") >= 4282000, "high").when(F.col("budget") >= 1902000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget")).otherwise(0)).alias("finance_budget"),
    )
    .filter(F.col("finance_budget") < 56)
    .select("budget_bucket", "finance_budget")
    .orderBy(F.col("finance_budget").desc())
)
