from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "London") & (F.col("location") != "Chiang Mai") & F.col("location").isNotNull())
    .groupBy(
        F.when(F.col("budget") >= 4064000, "high").when(F.col("budget") >= 1169000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)
