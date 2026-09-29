from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1324000, 1891000) & (F.col("budget") < 1774000))
    .groupBy(
        F.when(F.col("budget") >= 3435000, "high").when(F.col("budget") >= 1050000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") < 5)
    .drop("total_count")
)
