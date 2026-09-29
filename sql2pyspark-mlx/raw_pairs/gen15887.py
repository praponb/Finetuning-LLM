from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0))) / F.count("*")), 1).alias("pct_engineering"),
        F.sum("budget").alias("total_budget"),
        F.sum(F.when(F.col("location") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
    )
    .orderBy(F.col("chiang_mai_count").desc())
)
