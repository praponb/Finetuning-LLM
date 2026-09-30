from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(F.col("location").isin(["Chiang Mai", "Bangkok", "London"]) & ~F.col("location").isin(["Tokyo", "Berlin", "Chiang Mai"])))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4054000, "high").when(F.col("budget") >= 937000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .orderBy(F.col("chiang_mai_count").desc(), "budget_bucket")
)
