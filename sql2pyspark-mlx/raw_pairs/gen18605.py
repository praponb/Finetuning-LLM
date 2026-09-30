from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.month("opened_date").alias("opened_month"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg(
        F.count("sqft").alias("sqft_count"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
)
