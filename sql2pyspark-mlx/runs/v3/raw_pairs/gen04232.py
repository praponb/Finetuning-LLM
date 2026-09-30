from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "Singapore")
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.count("sqft").alias("sqft_count"))
    .filter(F.col("sqft_count") > 6)
    .select("opened_month", "sqft_count")
    .orderBy("opened_month")
)
