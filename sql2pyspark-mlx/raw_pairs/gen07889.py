from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 16495, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
    )
    .agg(
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.bit_and("sqft").alias("sqft_bit_and_2"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft_2"),
        F.stddev_samp("sqft").alias("sqft_stddev_2"),
    )
    .orderBy(F.col("sqft_stddev").desc())
)
