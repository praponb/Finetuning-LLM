from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "Tokyo")
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        "city",
        "region",
    )
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy(F.col("min_opened_date").desc(), "opened_month", "city", "region")
    .limit(1)
)
