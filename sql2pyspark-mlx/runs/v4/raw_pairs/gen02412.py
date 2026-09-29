from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4315000, "high").when(F.col("budget") >= 1427000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("avg_budget") >= 4007000)
    .orderBy("avg_budget")
)
