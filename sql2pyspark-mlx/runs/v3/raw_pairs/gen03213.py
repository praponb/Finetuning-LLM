from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3251000, "high").when(F.col("budget") >= 1074000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 4)
)
