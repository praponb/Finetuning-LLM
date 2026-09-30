from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.lower("store_name").like("%own%") & F.upper("store_name").like("%IAM%")) | F.col("city").isin(["Berlin", "New York"]))
    .groupBy(
        F.when(F.col("sqft") >= 12423, "high").when(F.col("sqft") >= 6115, "mid").otherwise("low").alias("sqft_bucket"),
        "city",
    )
    .agg(F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"))
)
