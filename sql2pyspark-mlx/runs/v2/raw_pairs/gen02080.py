from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(F.col("store_name").like("S%") & F.upper("store_name").like("%IAM%"))
    .withColumn("store_name_length", F.length("store_name"))
)

agg = (
    recent_stores
    .groupBy("region")
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.round(F.avg("store_name_length"), 2).alias("avg_store_name_length"),
    )
)

result = agg.select("region", "avg_store_name_length")
