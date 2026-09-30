from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.count("*").alias("row_count"),
    )
)
