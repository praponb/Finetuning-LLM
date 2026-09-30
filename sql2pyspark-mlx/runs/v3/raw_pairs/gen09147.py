from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3483000, "high").when(F.col("budget") >= 1625000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") >= 8)
    .orderBy("budget_bucket")
)
