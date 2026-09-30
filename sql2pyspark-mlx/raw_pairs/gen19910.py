from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2428000)
    .groupBy(
        F.when(F.col("budget") >= 4149000, "high").when(F.col("budget") >= 1881000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget")).otherwise(0)).alias("sales_budget"),
    )
    .filter(F.col("sales_budget") < 55)
)
