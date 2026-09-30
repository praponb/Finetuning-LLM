from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget")).otherwise(0)).alias("bangkok_budget"),
        F.countDistinct("budget").alias("unique_locations"),
        F.round(((100 * F.sum(F.when(F.col("location") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
    .filter(F.col("unique_locations") >= 2)
)
