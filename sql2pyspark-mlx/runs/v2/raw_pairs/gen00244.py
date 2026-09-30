from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.count("sqft").alias("sqft_count"))
    .filter(F.col("sqft_count") > 4)
    .select("opened_month", "sqft_count")
    .orderBy(F.col("sqft_count").desc())
)
