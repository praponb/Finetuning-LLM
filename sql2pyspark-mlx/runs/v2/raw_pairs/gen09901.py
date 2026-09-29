from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "Singapore") & F.col("budget").between(1396000, 2904000))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4340000, "high").when(F.col("budget") >= 878000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("budget").alias("budget_count"))
)
