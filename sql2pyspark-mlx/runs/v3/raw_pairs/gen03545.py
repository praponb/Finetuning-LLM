from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 1839000) | F.col("location").isin(["London", "New York", "Chiang Mai"]))
    .groupBy(
        F.when(F.col("budget") >= 4117000, "high").when(F.col("budget") >= 1150000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.count("budget").alias("budget_count"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("chiang_mai_count") >= 1)
)
