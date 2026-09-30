from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "region",
        F.when(F.col("city").isin(["Berlin", "Bangkok"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft")).otherwise(0)).alias("singapore_sqft"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
    .orderBy("region", "city_group")
    .limit(10)
)
