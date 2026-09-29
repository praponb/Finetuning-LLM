from pyspark.sql import functions as F

joined = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.units",
        "e.revenue",
        F.col("b.sqft"),
        F.col("b.region"),
    )
)

result = (
    joined
    .groupBy(F.when(F.col("sqft") >= 15533, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.sum(F.when(F.col("region") == "North", F.col("units")).otherwise(0)).alias("north_units"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.round(F.sum("revenue") / F.nullif(F.count(F.when(F.col("region") == "West", 1)), F.lit(0)), 2).alias("revenue_per_west"),
    )
    .orderBy("sqft_tier")
)
