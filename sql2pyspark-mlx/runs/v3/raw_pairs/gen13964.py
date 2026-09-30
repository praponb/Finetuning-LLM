from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2023-12-01")
    .select(
        "id",
        "store_name",
        "sqft",
        "city",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
    .orderBy(F.col("city").desc(), "id")
    .limit(20)
)
