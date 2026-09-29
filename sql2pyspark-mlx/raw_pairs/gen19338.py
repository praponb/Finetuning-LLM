from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Town%"))
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
    )
)
