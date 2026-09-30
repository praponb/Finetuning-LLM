from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4400000, "high").when(F.col("budget") >= 1302000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("total_budget") >= 21837000)
    .orderBy("total_budget")
)
