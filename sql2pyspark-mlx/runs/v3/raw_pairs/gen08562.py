from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Sales") | F.col("location").isin(["New York", "Tokyo", "Bangkok"]))
    .groupBy(
        F.when(F.col("budget") >= 4022000, "high").when(F.col("budget") >= 1556000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
    )
)
