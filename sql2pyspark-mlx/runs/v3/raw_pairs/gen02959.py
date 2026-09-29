from pyspark.sql import functions as F

base = (
    df_departments
    .filter(~F.col("location").isin(["Bangkok", "Tokyo", "Berlin"]) | (F.col("dept_name") == "Marketing"))
)

location_summary = (
    base
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3890000, "high").when(F.col("budget") >= 1349000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0))) / F.count("*")), 1).alias("pct_finance"),
    )
)

result = location_summary.orderBy(F.col("total_budget").desc())
