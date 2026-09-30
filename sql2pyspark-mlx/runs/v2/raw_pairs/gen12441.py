from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") == 9308) | F.col("store_name").like("%Mall%"))
    .groupBy("city", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.avg("sqft").alias("avg_sqft"),
    )
)
