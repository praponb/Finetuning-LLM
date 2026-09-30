from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 1650000) & ~F.col("dept_name").isin(["Finance", "Legal", "Sales"]))
    .groupBy(
        F.when(F.col("budget") >= 3732000, "high").when(F.col("budget") >= 1390000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count("*").alias("row_count"),
    )
)
