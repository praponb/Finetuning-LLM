from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3517000, "high").when(F.col("budget") >= 1499000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Sales", 1).otherwise(0)).alias("sales_count"),
    )
    .filter(F.col("sales_count") >= 59)
)
