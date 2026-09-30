from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "East")
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"), "city")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy("opened_month", F.col("city").desc())
    .limit(10)
)
