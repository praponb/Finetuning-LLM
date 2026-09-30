from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "London")
    .groupBy(
        F.when(F.col("location").isin(["London", "Chiang Mai"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.round(((100 * F.sum(F.when(F.col("location") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.sum(F.when(F.col("dept_name") == "Sales", 1).otherwise(0)).alias("sales_count"),
    )
    .filter(F.col("sales_count") < 56)
)
