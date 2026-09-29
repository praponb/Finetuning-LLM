from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4264000, "high").when(F.col("budget") >= 645000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
    .filter(F.col("total_budget") < 15202000)
    .orderBy("budget_bucket")
)
