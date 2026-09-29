from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(~((F.col("dept_name") == "Legal") & (F.col("location") == "Bangkok") & F.col("budget").between(1354000, 3876000)))
)

budget_bucket_summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3756000, "high").when(F.col("budget") >= 1268000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("budget").alias("budget_count"))
)

result = (
    budget_bucket_summary
    .filter(F.col("budget_count") > 8)
    .select("budget_bucket", "budget_count")
)
