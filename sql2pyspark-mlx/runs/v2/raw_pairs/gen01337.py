from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "Tokyo")
    .groupBy("city", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
    .orderBy(F.col("min_sqft").desc())
)
