from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 1990000) & ((F.col("budget") <= 2014000) | (F.col("budget") > 1253000)))
    .groupBy(
        F.when(F.col("budget") >= 3332000, "high").when(F.col("budget") >= 1769000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_count"))
    .orderBy(F.col("new_york_count").desc())
)
