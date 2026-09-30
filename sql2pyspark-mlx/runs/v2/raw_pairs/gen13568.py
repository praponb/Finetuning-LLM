from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(F.col("city").isin(["Chiang Mai", "Singapore", "New York"]) & (F.col("sqft") <= 15322))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
)

stores_stats = (
    recent_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.max("opened_date").alias("last_opened_date"))
)

result = stores_stats.select("opened_year", "last_opened_date")
