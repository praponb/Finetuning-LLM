from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Marketing", "Finance"]))
    .groupBy(
        F.when(F.col("budget") >= 4222000, "high").when(F.col("budget") >= 1477000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
    )
    .filter(F.col("pct_singapore") > 24)
)
