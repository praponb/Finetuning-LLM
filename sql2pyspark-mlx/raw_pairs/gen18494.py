from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 13371, "LARGE").otherwise("SMALL").alias("sqft_tier"),
        "region",
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.countDistinct("region").alias("distinct_regions"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.avg(F.when(F.col("region") == "South", F.col("sqft"))).alias("south_avg_sqft"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
    )
)
