from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sum("sqft").alias("total_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
)
