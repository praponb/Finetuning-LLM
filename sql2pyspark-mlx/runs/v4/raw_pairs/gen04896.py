from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3469000, "high").when(F.col("budget") >= 1484000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
    .filter(F.col("total_budget") > 9749000)
    .orderBy("budget_bucket")
)
