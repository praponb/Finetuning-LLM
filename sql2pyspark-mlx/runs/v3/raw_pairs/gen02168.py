from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.count("budget").alias("budget_count"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
    )
    .orderBy(F.col("budget_count").desc(), F.col("budget_range").desc())
)
