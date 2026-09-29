from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.countDistinct("sqft").alias("unique_regions"),
        F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") >= 13800)
    .drop("min_sqft")
    .orderBy("latest_opened_date", "opened_month")
)
