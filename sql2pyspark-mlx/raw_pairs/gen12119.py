from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 3123000)
    .groupBy(
        F.when(F.col("budget") >= 4391000, "high").when(F.col("budget") >= 1950000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
)
