from pyspark.sql import functions as F

base = df_stores.filter(F.length("store_name") >= 12)

stores_stats = (
    base
    .groupBy("city")
    .agg(
        F.count("*").alias("total_count"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
)

result = stores_stats.orderBy(F.col("city").desc())
