from pyspark.sql import functions as F

recent_stores = df_stores.filter(F.col("city") != "Tokyo")

opened_month_summary = (
    recent_stores
    .groupBy(F.month("opened_date").alias("opened_month"), "city")
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)

result = (
    opened_month_summary
    .filter(F.col("sqft_range") >= 80)
    .select("opened_month", "city", "sqft_range")
)
