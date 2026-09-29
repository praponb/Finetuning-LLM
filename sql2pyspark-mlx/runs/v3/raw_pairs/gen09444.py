from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum("sqft").alias("total_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
)
