from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3845000, "high").when(F.col("budget") >= 1300000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 4)
    .orderBy("budget_bucket")
)
