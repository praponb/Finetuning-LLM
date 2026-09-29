from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull() & (F.col("sqft") != 11356))
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
    .orderBy("city")
)
