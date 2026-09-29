from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4408000, "high").when(F.col("budget") >= 1413000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.max("budget").alias("max_budget"),
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
        F.sum("budget").alias("total_budget"),
    )
    .orderBy(F.col("total_budget").desc(), "dept_name")
)
