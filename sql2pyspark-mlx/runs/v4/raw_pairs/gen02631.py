from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter((F.col("city") == "Chiang Mai") & (F.col("opened_date") <= "2024-12-03"))
)

summary = (
    valid_stores
    .groupBy("city")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.avg("sqft").alias("avg_sqft"),
    )
)

result = (
    summary
    .filter(F.col("avg_sqft") < 11600)
    .orderBy(F.col("city").desc())
)
