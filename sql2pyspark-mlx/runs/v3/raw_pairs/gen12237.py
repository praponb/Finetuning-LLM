from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.max("opened_date").alias("latest_opened_date"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
)
