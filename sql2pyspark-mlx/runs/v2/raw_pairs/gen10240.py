from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Engineering"]))
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3296000, "high").when(F.col("budget") >= 645000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 6)
    .orderBy(F.col("cnt").desc())
)
