from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "London") & (F.year("opened_date") >= 2024))
    .groupBy("city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sum(F.when(F.col("region") == "Central", 1).otherwise(0)).alias("central_count"),
        F.count("sqft").alias("sqft_count"),
    )
)
