from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1399000)
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4444000, "high").when(F.col("budget") >= 968000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
        F.sum("budget").alias("total_budget"),
    )
    .orderBy(F.col("total_budget").desc())
)
