from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "London")
    .groupBy(
        F.when(F.col("budget") >= 3171000, "high").when(F.col("budget") >= 1711000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.max("budget").alias("max_budget"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("budget_bucket").desc(), F.col("max_budget").desc())
)
