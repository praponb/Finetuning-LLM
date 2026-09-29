from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3782000, "high").when(F.col("budget") >= 1562000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("budget_range") > 77)
)
