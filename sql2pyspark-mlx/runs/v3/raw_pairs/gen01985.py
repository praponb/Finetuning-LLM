from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull() | F.col("location").isNull())
    .groupBy(
        F.when(F.col("budget") >= 3120000, "high").when(F.col("budget") >= 596000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.sum(F.when(F.col("location") == "Berlin", F.col("budget")).otherwise(0)).alias("berlin_budget"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
    .orderBy(F.col("berlin_budget").desc())
)
