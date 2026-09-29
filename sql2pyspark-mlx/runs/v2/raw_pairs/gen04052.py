from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["HR", "Finance"]))
    .groupBy(
        F.when(F.col("budget") >= 3766000, "high").when(F.col("budget") >= 683000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.avg("budget").alias("avg_budget"))
)
