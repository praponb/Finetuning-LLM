from pyspark.sql import functions as F

result = (
    df_stores
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.count("sqft").alias("sqft_count"),
    )
    .orderBy(F.col("sqft_count").desc(), "latest_opened_date")
)
