from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("city").isin(["Singapore", "Bangkok"]), "group_a").otherwise("other").alias("city_group"),
        "region",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft")).otherwise(0)).alias("singapore_sqft"),
    )
    .filter(F.col("singapore_sqft") < 29)
    .drop("singapore_sqft")
    .orderBy("cnt", F.col("east_sqft").desc(), "city_group", "region")
    .limit(5)
)
