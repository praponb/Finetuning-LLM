from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(~F.col("region").isin(["West", "North", "Central"]))
    .withColumn("opened_date_period", F.date_trunc("month", "opened_date"))
)

stores_stats = (
    filtered_stores
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("sqft").alias("sqft_count"),
    )
)

result = (
    stores_stats
    .filter(F.col("sqft_count") >= 8)
    .select("city", "city_list", "sqft_count")
)
