from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2024-12-08")
    .groupBy("region")
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .orderBy("latest_opened_date")
)
