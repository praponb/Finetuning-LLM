from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.count("*").alias("store_count"),
    )
    .orderBy("opened_month")
)
