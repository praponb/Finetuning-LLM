from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
    )
    .filter(F.col("min_sqft") < 8500)
    .select("opened_month", "min_sqft", "berlin_count")
)
