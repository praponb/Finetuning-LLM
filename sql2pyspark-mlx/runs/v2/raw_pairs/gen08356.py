from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3774000, "high").when(F.col("budget") >= 1957000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.max("budget").alias("max_budget"),
        F.count("budget").alias("budget_count"),
        F.countDistinct("budget").alias("unique_locations"),
    )
    .filter(F.col("unique_locations") < 1)
    .drop("unique_locations")
    .orderBy("budget_bucket", F.col("budget_count").desc())
    .limit(50)
)
