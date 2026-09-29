from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3694000, "high").when(F.col("budget") >= 1093000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
    )
    .orderBy(F.col("cnt").desc(), F.col("berlin_count").desc())
)
