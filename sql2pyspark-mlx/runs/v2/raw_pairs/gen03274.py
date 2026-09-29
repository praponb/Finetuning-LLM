from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "New York") & (F.col("dept_name") == "HR"))
    .groupBy(
        F.when(F.col("budget") >= 3422000, "high").when(F.col("budget") >= 794000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget")).otherwise(0)).alias("tokyo_budget"),
    )
)
