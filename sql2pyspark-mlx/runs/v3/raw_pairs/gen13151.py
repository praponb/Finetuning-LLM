from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("store_name").like("%Centr%") | ~F.col("store_name").like("%Cen%"))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sum("sqft").alias("total_sqft"),
    )
    .orderBy(F.col("stddev_sqft").desc())
)
