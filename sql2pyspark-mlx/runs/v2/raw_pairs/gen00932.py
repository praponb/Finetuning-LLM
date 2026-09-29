from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location") != "Tokyo")
    .withColumn("location_title", F.initcap(F.trim("location")))
)

budget_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3102000, "high").when(F.col("budget") >= 1944000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
)

result = budget_bucket_summary
