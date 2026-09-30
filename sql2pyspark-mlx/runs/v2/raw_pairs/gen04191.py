from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Legal") & (F.col("budget") >= 3824000))
    .groupBy(
        F.when(F.col("budget") >= 3379000, "high").when(F.col("budget") >= 1717000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
)
