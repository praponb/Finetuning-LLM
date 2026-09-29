from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.max("opened_date").alias("latest_opened_date"),
    )
)
