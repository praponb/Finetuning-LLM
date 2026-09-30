from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") != 12201) & F.col("region").isNotNull())
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
)
