from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3162000, "high").when(F.col("budget") >= 1514000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
        F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0)).alias("marketing_count"),
    )
    .filter(F.col("engineering_count") >= 41)
)
