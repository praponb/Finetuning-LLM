from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.opened_date")), "left_semi")
)

result = (
    not_flagged
    .groupBy(F.when(F.col("sqft") >= 16698, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.countDistinct("region").alias("distinct_regions"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "East", 1)), F.lit(0)), 2).alias("sqft_per_east"),
    )
    .filter(F.col("sqft_per_east") > 0)
    .orderBy("sqft_p50")
)
