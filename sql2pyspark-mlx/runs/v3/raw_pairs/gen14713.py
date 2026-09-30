from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.min("sqft").alias("min_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.min("opened_date").alias("first_opened_date"),
    )
)
