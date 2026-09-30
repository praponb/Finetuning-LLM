from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3257000, "high").when(F.col("budget") >= 1136000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") > 58)
    .select("budget_bucket", "budget_range")
)
