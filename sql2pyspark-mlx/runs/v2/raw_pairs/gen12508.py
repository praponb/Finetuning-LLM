from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1546000, 3341000))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3400000, "high").when(F.col("budget") >= 1668000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
    .filter(F.col("stddev_budget") > 72)
    .select("location", "budget_bucket", "stddev_budget")
)
