from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter((F.col("sqft") > 14586) & (F.year("opened_date") == 2023))
    .withColumn("sqft_filled", F.coalesce("sqft", F.lit(0)))
)

agg = (
    valid_stores
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
    )
)

result = agg.select("city", "region_list", "north_sqft")
