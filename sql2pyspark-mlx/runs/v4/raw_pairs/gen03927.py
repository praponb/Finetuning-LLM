from pyspark.sql import functions as F

result = (
    df_stores
    .cube(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"), "city")
    .agg(
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.avg("sqft").alias("avg_sqft"),
        F.sum(F.when(F.col("region") == "Central", F.col("sqft"))).alias("central_total_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("central_total_sqft").desc())
)
