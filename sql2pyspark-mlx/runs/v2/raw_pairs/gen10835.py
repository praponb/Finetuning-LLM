from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3872000, "high").when(F.col("budget") >= 1907000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Sales", 1).otherwise(0)).alias("sales_count"),
    )
    .filter(F.col("sales_count") >= 37)
    .orderBy("budget_bucket")
)
