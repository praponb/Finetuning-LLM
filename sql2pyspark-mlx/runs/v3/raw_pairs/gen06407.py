from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNotNull())
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.avg("sqft").alias("avg_sqft"),
    )
)
