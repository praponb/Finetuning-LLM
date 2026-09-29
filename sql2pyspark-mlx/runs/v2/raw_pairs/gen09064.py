from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNotNull())
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
    )
    .filter(F.col("sqft_count") > 8)
    .select("opened_month", "sqft_count", "south_count")
    .orderBy("south_count", F.col("opened_month").desc())
)
