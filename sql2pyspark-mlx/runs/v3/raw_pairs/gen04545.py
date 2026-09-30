from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 1973000) & (F.col("budget") <= 2380000))
    .groupBy(
        F.when(F.col("budget") >= 3346000, "high").when(F.col("budget") >= 1816000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
)
