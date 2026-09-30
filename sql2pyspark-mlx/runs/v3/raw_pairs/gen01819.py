from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4403000, "high").when(F.col("budget") >= 1344000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
        "location",
    )
    .agg(
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
)
