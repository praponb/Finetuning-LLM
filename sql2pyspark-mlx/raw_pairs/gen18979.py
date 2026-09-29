from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3581000, "high").when(F.col("budget") >= 1530000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"))
)
