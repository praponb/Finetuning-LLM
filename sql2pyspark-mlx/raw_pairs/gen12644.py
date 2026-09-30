from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Bangkok")
    .groupBy(
        F.when(F.col("budget") >= 3352000, "high").when(F.col("budget") >= 696000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.max("budget").alias("max_budget"))
    .filter(F.col("max_budget") >= 1228000)
)
