from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3240000)
    .groupBy(
        F.when(F.col("budget") >= 3956000, "high").when(F.col("budget") >= 1146000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
    .orderBy("pct_bangkok")
)
