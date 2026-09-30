from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter(F.col("store_name").isNull() | F.col("city").isin(["London", "Chiang Mai", "New York"]))
)

summary = (
    valid_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
)

result = summary.filter(F.col("cnt") >= 7)
