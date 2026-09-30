from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1300000, 3427000))
    .groupBy("location")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
    )
    .filter(F.col("pct_bangkok") > 76)
    .select("location", "pct_bangkok", "london_budget")
)
