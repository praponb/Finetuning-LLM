from pyspark.sql import functions as F

result = (
    df_departments
    .filter((~F.col("location").isin(["New York", "Tokyo", "Chiang Mai"]) & (F.col("dept_name") != "HR")) | F.col("location").isin(["New York", "Bangkok", "Singapore"]))
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3151000, "high").when(F.col("budget") >= 1602000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget")).otherwise(0)).alias("finance_budget"),
    )
    .orderBy("dept_name")
)
