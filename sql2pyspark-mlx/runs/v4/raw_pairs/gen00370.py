from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Marketing", "Sales"]) & F.col("location").isin(["New York", "London", "Berlin"]))
)

budget_bucket_summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 4013000, "high").when(F.col("budget") >= 1433000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.countDistinct("budget").alias("unique_locations"),
    )
)

result = budget_bucket_summary
