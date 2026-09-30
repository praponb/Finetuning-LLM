from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4213000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
        F.count_if(F.col("dept_name") == "Engineering").alias("engineering_events"),
        F.count_if(F.col("location") == "Bangkok").alias("bangkok_events"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
    )
    .filter(F.col("berlin_count") < 1)
)
