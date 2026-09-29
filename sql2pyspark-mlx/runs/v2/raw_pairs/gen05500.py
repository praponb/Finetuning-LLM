from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3962000, "high").when(F.col("budget") >= 2011000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"),
    )
    .filter(F.col("min_budget") > 3011000)
    .orderBy(F.col("legal_count").desc(), "location")
)
