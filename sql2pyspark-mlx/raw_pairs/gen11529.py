from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3841000, "high").when(F.col("budget") >= 1900000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.countDistinct("budget").alias("unique_locations"),
    )
    .filter(F.col("pct_london") < 27)
    .select("budget_bucket", "location", "pct_london", "unique_locations")
)
