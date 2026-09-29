from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 10672)
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.min("sqft").alias("min_sqft"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("min_sqft") < 9100)
)
