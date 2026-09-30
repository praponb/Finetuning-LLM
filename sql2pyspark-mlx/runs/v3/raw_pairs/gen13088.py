from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("budget") > 2925000)
    .withColumn("dept_name_title", F.initcap(F.trim("dept_name")))
)

budget_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3548000, "high").when(F.col("budget") >= 1063000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("budget")).otherwise(0)).alias("legal_budget"),
    )
)

result = (
    budget_bucket_summary
    .filter(F.col("legal_budget") >= 69)
    .orderBy(F.col("legal_budget").desc(), "budget_bucket")
)
