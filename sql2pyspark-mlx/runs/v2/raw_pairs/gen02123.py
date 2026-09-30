from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "HR"]))
    .groupBy(
        F.when(F.col("budget") >= 3798000, "high").when(F.col("budget") >= 1671000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_count"))
)
