from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Chiang Mai")
    .groupBy("region", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.count("sqft").alias("sqft_count"),
        F.max("opened_date").alias("latest_opened_date"),
    )
)
