from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull() & F.col("dept_name").isin(["Finance", "Legal"]) & F.col("budget").isNotNull())
    .groupBy(
        F.when(F.col("budget") >= 3117000, "high").when(F.col("budget") >= 660000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
    )
)
