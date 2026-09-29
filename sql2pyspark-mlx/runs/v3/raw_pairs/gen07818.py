from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Singapore") & F.col("dept_name").isin(["HR", "Sales", "Finance"]))
    .groupBy(
        F.when(F.col("budget") >= 4355000, "high").when(F.col("budget") >= 1075000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
    )
    .filter(F.col("london_count") >= 31)
    .orderBy("dept_name", "budget_bucket")
    .limit(1)
)
