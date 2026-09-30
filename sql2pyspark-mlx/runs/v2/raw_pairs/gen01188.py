from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3340000, "high").when(F.col("budget") >= 1477000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
    .filter(F.col("avg_budget") < 25)
    .orderBy(F.col("budget_bucket").desc())
    .limit(10)
)
