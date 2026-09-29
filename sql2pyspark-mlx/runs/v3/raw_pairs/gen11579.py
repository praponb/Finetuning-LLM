from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("sqft").isNull())
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
)

stores_stats = (
    base
    .groupBy(
        F.date_trunc("MONTH", "opened_date_month_end").alias("opened_month_end_month"),
    )
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)

result = (
    stores_stats
    .select("opened_month_end_month", "sqft_range")
    .orderBy(F.col("opened_month_end_month").desc())
    .limit(1)
)
