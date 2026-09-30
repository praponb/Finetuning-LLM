from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter((F.col("city") == "Chiang Mai") & ~F.col("city").isin(["Berlin", "Bangkok"]) & (F.col("city") == "Bangkok"))
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
)

rating_bucket_summary = (
    recent_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"))
)

result = rating_bucket_summary.orderBy("uk_count")
