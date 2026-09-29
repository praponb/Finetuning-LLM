from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "Tokyo")
    .groupBy(
        "city",
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        "region",
    )
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy(F.col("min_opened_date").desc(), "city", "opened_month", "region")
    .limit(1)
)
