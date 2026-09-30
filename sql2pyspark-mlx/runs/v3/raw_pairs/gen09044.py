from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.when(F.col("sqft") >= 15898, "high").when(F.col("sqft") >= 3630, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.count("*").alias("store_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .orderBy("city_values")
)
