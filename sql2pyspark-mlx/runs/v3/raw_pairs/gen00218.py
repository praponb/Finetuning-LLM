from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter((F.length("store_name") < 8) & F.col("region").isNull())
    .withColumn("store_name_region", F.concat("store_name", F.lit(" - "), "region"))
)

stores_stats = (
    valid_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.count("sqft").alias("sqft_count"),
    )
)

result = (
    stores_stats
    .select("opened_month", "sqft_count")
    .orderBy("opened_month", F.col("sqft_count").desc())
)
