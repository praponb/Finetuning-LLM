from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.year("opened_date") == 2023) & (F.col("region") != "East") & (F.col("sqft") <= 8366))
)

stores_stats = (
    src
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.max("opened_date").alias("last_opened_date"))
)

result = stores_stats
