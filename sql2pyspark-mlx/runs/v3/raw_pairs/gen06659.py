from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4274000, "high").when(F.col("budget") >= 2003000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
    )
    .filter(F.col("engineering_count") > 77)
    .orderBy(F.col("total_budget").desc())
)
