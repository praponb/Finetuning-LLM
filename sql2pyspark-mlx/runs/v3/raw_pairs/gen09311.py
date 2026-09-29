from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3707000, "high").when(F.col("budget") >= 1039000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("budget").alias("budget_count"))
    .orderBy("budget_bucket")
)
