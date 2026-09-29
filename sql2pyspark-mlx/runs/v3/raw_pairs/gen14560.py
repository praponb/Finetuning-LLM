from pyspark.sql import functions as F

base = (
    df_departments
    .filter((F.col("dept_name") != "Legal") & ~F.col("location").isin(["London", "Berlin", "Tokyo"]) & ~F.col("location").isin(["Chiang Mai", "Bangkok"]))
    .withColumn("is_singapore", F.when(F.col("location") == "Singapore", 1).otherwise(0))
)

summary = (
    base
    .groupBy(
        "dept_name",
        F.when(F.col("is_singapore") >= 90, "high").when(F.col("is_singapore") >= 32, "mid").otherwise("low").alias("is_singapore_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0))) / F.count("*")), 1).alias("pct_finance"),
        F.count("*").alias("total_count"),
    )
)

result = summary
