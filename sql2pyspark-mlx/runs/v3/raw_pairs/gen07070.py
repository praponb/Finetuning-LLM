from pyspark.sql import functions as F

src = df_stores.filter(F.col("region") == "West")

stores_stats = (
    src
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
)

result = stores_stats.filter(F.col("sqft_range") > 35)
