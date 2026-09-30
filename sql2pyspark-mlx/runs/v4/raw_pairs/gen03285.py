from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.avg(F.when(F.col("city") == "Singapore", F.col("rating"))).alias("singapore_avg_rating"),
        F.sum(F.when(F.col("country") == "JP", F.col("rating"))).alias("jp_total_rating"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
    )
    .orderBy(F.col("singapore_avg_rating").asc_nulls_last())
)
