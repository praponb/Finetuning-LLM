from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("location") == "Chiang Mai") & (F.col("budget") < 3775000))
)

budget_bucket_summary = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 4424000, "high").when(F.col("budget") >= 819000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
)

result = (
    budget_bucket_summary
    .filter(F.col("total_budget") < 9033000)
    .orderBy("budget_bucket")
)
