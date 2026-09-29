from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Engineering", "Sales"]) & ~F.col("dept_name").isin(["Finance", "Marketing"]))
    .groupBy(
        F.when(F.col("budget") >= 3993000, "high").when(F.col("budget") >= 1045000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("min_budget") < 3320000)
    .orderBy(F.col("min_budget").desc())
)
