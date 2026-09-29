from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Tokyo", "Singapore"]) & F.col("budget").between(3469000, 3666000) & (F.col("budget") > 2824000))
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
        F.sum(F.when(F.col("location") == "Berlin", F.col("budget")).otherwise(0)).alias("berlin_budget"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("max_budget") >= 1961000)
)
