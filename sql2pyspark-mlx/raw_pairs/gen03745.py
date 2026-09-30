from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~((F.col("location").isin(["Chiang Mai", "Tokyo", "Bangkok"]) & F.col("location").isin(["Berlin", "London", "Tokyo"])) | F.col("location").isin(["Chiang Mai", "Singapore", "New York"])))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3658000, "high").when(F.col("budget") >= 1657000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .orderBy(F.col("pct_berlin").desc())
)
