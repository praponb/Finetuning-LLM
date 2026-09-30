from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3178000, "high").when(F.col("budget") >= 1045000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"))
    .orderBy(F.col("budget_bucket").desc())
    .limit(1)
)
