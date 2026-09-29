from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Singapore") & F.col("location").isin(["Tokyo", "New York", "Bangkok"]))
    .groupBy(
        F.when(F.col("budget") >= 3571000, "high").when(F.col("budget") >= 2051000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .orderBy(F.col("budget_range").desc())
)
