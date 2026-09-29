from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
)
