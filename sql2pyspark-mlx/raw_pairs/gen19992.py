from pyspark.sql import functions as F

recent_stores = df_stores.filter(F.col("city").isin(["New York", "Berlin", "Singapore"]))

stores_stats = (
    recent_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)

result = (
    stores_stats
    .filter(F.col("sqft_range") < 56)
    .select("opened_month", "sqft_range")
    .orderBy(F.col("opened_month").desc())
    .limit(10)
)
