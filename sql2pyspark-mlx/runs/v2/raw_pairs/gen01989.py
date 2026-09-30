from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Legal", "Sales"]) & F.col("dept_name").isin(["Marketing", "Engineering"]))
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3163000, "high").when(F.col("budget") >= 1879000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
        F.min("budget").alias("min_budget"),
    )
    .filter(F.col("pct_bangkok") < 61)
    .orderBy(F.col("dept_name").desc(), F.col("pct_bangkok").desc())
)
