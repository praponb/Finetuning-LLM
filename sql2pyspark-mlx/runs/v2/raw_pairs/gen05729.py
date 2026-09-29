from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3854000, "high").when(F.col("budget") >= 1030000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("*").alias("department_count"))
    .filter(F.col("department_count") > 10)
    .select("budget_bucket", "department_count")
    .orderBy("budget_bucket")
)
