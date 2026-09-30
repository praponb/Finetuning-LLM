from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2023-05-01") & F.col("store_name").like("%Centr%"))
    .groupBy("region")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.sum("sqft").alias("total_sqft"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
    )
    .filter(F.col("pct_tokyo") > 46)
    .orderBy(F.col("pct_tokyo").desc())
)
