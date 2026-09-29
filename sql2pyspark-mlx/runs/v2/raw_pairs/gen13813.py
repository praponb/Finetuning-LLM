from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1540000, 2292000))
    .groupBy(
        F.when(F.col("budget") >= 4204000, "high").when(F.col("budget") >= 1080000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
)
