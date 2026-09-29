from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 13909)
    .groupBy("city", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
)
