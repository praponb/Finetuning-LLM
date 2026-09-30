from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sum(F.when(F.col("region") == "Central", 1).otherwise(0)).alias("central_count"),
        F.min("opened_date").alias("first_opened_date"),
    )
)
