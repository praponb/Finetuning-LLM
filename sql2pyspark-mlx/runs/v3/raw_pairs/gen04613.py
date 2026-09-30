from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3617000, "high").when(F.col("budget") >= 833000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
    )
    .filter(F.col("avg_budget") >= 67)
    .orderBy(F.col("avg_budget").desc(), "london_count")
)
