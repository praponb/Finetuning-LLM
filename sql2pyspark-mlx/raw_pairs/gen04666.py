from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12834, "high").when(F.col("sqft") >= 6689, "mid").otherwise("low").alias("sqft_bucket"),
        F.year("opened_date").alias("opened_year"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.sum(F.when(F.col("region") == "West", 1).otherwise(0)).alias("west_count"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .filter(F.col("west_count") >= 35)
)
