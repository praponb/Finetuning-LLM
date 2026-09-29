from pyspark.sql import functions as F

src = (
    df_departments
    .filter((F.col("dept_name") == "Engineering") & F.col("dept_name").isin(["Marketing", "Sales", "Engineering"]))
)

budget_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("budget") >= 3372000, "high").when(F.col("budget") >= 1492000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.max("budget").alias("max_budget"),
    )
)

result = (
    budget_bucket_summary
    .select("budget_bucket", "total_count", "max_budget")
    .orderBy("budget_bucket")
)
