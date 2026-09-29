from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 14258)
    .select(
        "sqft",
        "opened_date",
        "region",
        F.lower("city").alias("city_lower"),
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)
