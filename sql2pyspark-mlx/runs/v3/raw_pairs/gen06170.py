from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12964, "high").when(F.col("sqft") >= 7018, "mid").otherwise("low").alias("sqft_bucket"),
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
    )
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("sqft")).otherwise(0)).alias("chiang_mai_sqft"),
    )
    .orderBy("chiang_mai_sqft")
)
