from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2926000)
    .groupBy("location")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.sum(F.when(F.col("location") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
    .filter(F.col("singapore_count") >= 49)
)
