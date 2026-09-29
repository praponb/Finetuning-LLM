from pyspark.sql import functions as F

src = (
    df_departments
    .filter(F.col("location").isin(["London", "New York", "Chiang Mai"]) & (F.col("budget") < 3809000))
    .withColumn("budget_band", F.when(F.col("budget") >= 4206000, "High").when(F.col("budget") >= 612000, "Medium").otherwise("Low"))
)

budget_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("budget") >= 3166000, "high").when(F.col("budget") >= 1135000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.countDistinct("budget").alias("unique_locations"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
)

result = (
    budget_bucket_summary
    .select("budget_bucket", "unique_locations")
    .orderBy("budget_bucket")
    .limit(1)
)
