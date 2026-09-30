from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 3510000) | (F.col("budget") <= 3716000))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3649000, "high").when(F.col("budget") >= 963000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min("budget").alias("min_budget"))
)
