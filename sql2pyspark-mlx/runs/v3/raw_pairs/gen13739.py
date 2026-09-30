from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .groupBy(
        F.when(F.col("budget") >= 3549000, "high").when(F.col("budget") >= 866000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"))
    .orderBy(F.col("budget_bucket").desc())
)
