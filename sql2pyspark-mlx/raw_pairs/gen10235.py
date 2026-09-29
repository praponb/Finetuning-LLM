from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") != 10132) | (F.col("sqft") < 12091))
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
)
