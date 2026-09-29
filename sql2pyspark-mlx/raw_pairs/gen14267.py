from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 3926000) & F.col("dept_name").isin(["Legal", "Finance"]))
    .groupBy(
        F.when(F.col("budget") >= 3809000, "high").when(F.col("budget") >= 1786000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .orderBy(F.col("bangkok_count").desc())
)
