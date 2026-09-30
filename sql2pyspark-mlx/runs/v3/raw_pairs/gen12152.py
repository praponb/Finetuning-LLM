from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 3862000) | F.col("location").isin(["Chiang Mai", "Bangkok", "London"]))
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
        F.count("*").alias("department_count"),
    )
    .orderBy(F.col("singapore_count").desc())
)
