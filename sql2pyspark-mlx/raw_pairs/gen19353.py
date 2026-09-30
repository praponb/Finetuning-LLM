from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "Tokyo"]) & (F.col("location") == "Singapore"))
)

summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3674000, "high").when(F.col("budget") >= 988000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("location").isin(["Berlin", "Chiang Mai"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
    )
)

result = (
    summary
    .select("budget_bucket", "engineering_count")
    .orderBy(F.col("budget_bucket").desc())
)
