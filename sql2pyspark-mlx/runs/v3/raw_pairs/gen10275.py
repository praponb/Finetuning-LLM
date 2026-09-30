from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3953000, "high").when(F.col("budget") >= 1681000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy("budget_bucket", "location")
)
