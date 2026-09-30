from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 10847)
    .groupBy("city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
)
