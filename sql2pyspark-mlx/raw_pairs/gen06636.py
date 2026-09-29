from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4315000, "high").when(F.col("budget") >= 1180000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.avg("budget").alias("avg_budget"))
    .filter(F.col("avg_budget") >= 1664000)
    .orderBy("budget_bucket")
)
