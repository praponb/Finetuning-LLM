from pyspark.sql import functions as F

base = (
    df_departments
    .filter((F.col("dept_name") != "Marketing") & (F.col("location") == "Chiang Mai"))
)

budget_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3714000, "high").when(F.col("budget") >= 1659000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count("*").alias("num_departments"),
        F.max("budget").alias("max_budget"),
    )
)

result = budget_bucket_summary
