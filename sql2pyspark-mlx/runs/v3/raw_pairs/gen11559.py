from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([2, 7, 11]) & (F.col("city") == "Chiang Mai"))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") < 13200)
    .orderBy("opened_year")
)
