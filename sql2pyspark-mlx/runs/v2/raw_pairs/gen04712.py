from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Marketing", "Finance"]))
    .groupBy(
        F.when(F.col("budget") >= 3537000, "high").when(F.col("budget") >= 2042000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
    .filter(F.col("stddev_budget") > 77)
)
