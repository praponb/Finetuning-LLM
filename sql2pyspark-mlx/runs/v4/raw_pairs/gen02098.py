from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Engineering", "HR"]))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4099000, "high").when(F.col("budget") >= 675000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget")).otherwise(0)).alias("finance_budget"),
    )
    .orderBy("location")
)
