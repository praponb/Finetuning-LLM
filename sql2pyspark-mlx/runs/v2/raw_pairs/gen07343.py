from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 15478)
    .groupBy("city", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") >= 9400)
)
