from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNotNull())
    .groupBy(
        F.when(F.col("age") >= 55, "high").when(F.col("age") >= 43, "mid").otherwise("low").alias("age_bucket"),
        "city",
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
        F.count("age").alias("age_count"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
    .orderBy("bangkok_count", "age_bucket", "city")
    .limit(10)
)
