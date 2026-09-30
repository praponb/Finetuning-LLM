from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2094000)
    .groupBy(
        F.when(F.col("budget") >= 3343000, "high").when(F.col("budget") >= 1586000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
)
