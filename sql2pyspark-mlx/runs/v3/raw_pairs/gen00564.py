from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3629000, "high").when(F.col("budget") >= 935000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
    )
    .orderBy(F.col("legal_count").desc(), F.col("location").desc(), "budget_bucket")
    .limit(50)
)
