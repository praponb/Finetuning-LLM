from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Marketing"]) & F.col("location").isin(["Chiang Mai", "Bangkok"]))
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3526000, "high").when(F.col("budget") >= 1185000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min("budget").alias("min_budget"))
    .orderBy(F.col("min_budget").desc())
)
