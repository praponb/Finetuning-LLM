from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter((F.col("sqft") <= 9130) & F.col("sqft").isNotNull() & F.col("store_name").like("Ma%"))
)

stores_stats = (
    filtered_stores
    .groupBy("region")
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
)

result = (
    stores_stats
    .filter(F.col("sqft_range") >= 80)
    .select("region", "stddev_sqft")
    .orderBy(F.col("region").desc())
    .limit(5)
)
