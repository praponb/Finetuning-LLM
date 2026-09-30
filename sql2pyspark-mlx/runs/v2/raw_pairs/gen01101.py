from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "region")
    .agg(F.count("sqft").alias("sqft_count"))
    .filter(F.col("sqft_count") > 9)
    .select("opened_month", "region", "sqft_count")
    .orderBy("sqft_count", "opened_month", "region")
    .limit(5)
)
