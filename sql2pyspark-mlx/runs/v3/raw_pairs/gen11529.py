from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3641000, "high").when(F.col("budget") >= 1072000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_legal"),
        F.max("budget").alias("max_budget"),
    )
    .orderBy(F.col("pct_legal").desc())
)
