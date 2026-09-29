from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3053000, "high").when(F.col("budget") >= 791000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.sum("budget").alias("total_budget"),
    )
)
