from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3842000, "high").when(F.col("budget") >= 1725000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_legal"),
        F.avg("budget").alias("avg_budget"),
    )
    .orderBy("pct_legal")
)
