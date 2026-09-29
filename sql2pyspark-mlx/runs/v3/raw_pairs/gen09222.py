from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2024-08-23")
    .select(
        "city",
        "store_name",
        "region",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
    .orderBy(F.col("city").desc())
)
