from pyspark.sql import functions as F

recent_stores = df_stores.filter(F.col("sqft").isNotNull())

agg = (
    recent_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
)

result = (
    agg
    .select("opened_year", "sqft_range", "stddev_sqft")
    .orderBy(F.col("stddev_sqft").desc())
)
