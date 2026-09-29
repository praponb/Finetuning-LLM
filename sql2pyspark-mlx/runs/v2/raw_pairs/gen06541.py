from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Singapore", "Chiang Mai", "Bangkok"]))
    .groupBy(
        F.when(F.col("budget") >= 3130000, "high").when(F.col("budget") >= 684000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget")).otherwise(0)).alias("tokyo_budget"),
    )
)
