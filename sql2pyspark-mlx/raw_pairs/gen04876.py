from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "South")
    .groupBy(
        F.when(F.col("region").isin(["Central", "North"]), "tier_1").otherwise("other").alias("region_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "North", 1).otherwise(0))) / F.count("*")), 1).alias("pct_north"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .orderBy(F.col("pct_north").desc())
)
