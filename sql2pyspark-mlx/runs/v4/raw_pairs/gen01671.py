from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.date_trunc("month", "opened_date").alias("activity_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.bit_or("sqft").alias("sqft_bit_or"),
    )
    .orderBy("sqft_bit_or")
)
