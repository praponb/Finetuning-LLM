from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3713000, "high").when(F.col("budget") >= 1672000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
    .orderBy("budget_bucket")
)
