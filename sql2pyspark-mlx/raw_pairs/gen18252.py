from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4213000, "high").when(F.col("budget") >= 1355000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget")).otherwise(0)).alias("finance_budget"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"),
    )
    .filter(F.col("legal_count") < 4)
)
