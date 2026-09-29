from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") != "Finance") & ~F.col("dept_name").isin(["HR", "Finance"]) & ~F.col("location").isin(["Chiang Mai", "New York"]))
    .groupBy(
        F.when(F.col("budget") >= 4302000, "high").when(F.col("budget") >= 1328000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
        "dept_name",
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") > 23)
    .orderBy("dept_name", F.col("budget_range").desc())
)
