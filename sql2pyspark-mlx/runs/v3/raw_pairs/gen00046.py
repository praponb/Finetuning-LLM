from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3264000, "high").when(F.col("budget") >= 1602000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
    .filter(F.col("stddev_budget") < 58)
    .select("budget_bucket", "location", "stddev_budget")
    .orderBy("location")
)
