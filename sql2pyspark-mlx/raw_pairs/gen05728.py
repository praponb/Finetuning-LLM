from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1209000)
    .groupBy(
        F.when(F.col("budget") >= 3045000, "high").when(F.col("budget") >= 802000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .filter(F.col("chiang_mai_count") < 4)
    .orderBy(F.col("chiang_mai_count").desc())
)
