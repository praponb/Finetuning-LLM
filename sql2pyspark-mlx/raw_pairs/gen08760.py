from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Marketing")
    .groupBy(
        F.when(F.col("budget") >= 3271000, "high").when(F.col("budget") >= 1489000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 6)
    .select("budget_bucket", "row_count")
)
