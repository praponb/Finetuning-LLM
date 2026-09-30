from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4436000, "high").when(F.col("budget") >= 872000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"))
    .filter(F.col("berlin_count") > 9)
    .select("budget_bucket", "dept_name", "berlin_count")
)
