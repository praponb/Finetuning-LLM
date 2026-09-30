from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4401000, "high").when(F.col("budget") >= 1051000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.avg("budget").alias("avg_budget"),
    )
)
